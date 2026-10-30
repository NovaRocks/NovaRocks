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

//! Real catalog callback + Avro discovery + typed split-open component tests.
//! The explicit inline CPU/MIO fixtures are not a physical QB/WC exit oracle.

use super::prepared_source_phase_support::{IoFixture, OriginalScope, PhasePort, written};
use super::*;
use crate::catalog::catalog_request::CatalogPublicationRequest;
use crate::catalog::error::{CatalogOutcome, CatalogUnsupported};
use crate::catalog::{CatalogNamespaceName, CatalogTableName, NovaRocksCatalog, SnapshotLoading};
use crate::loaded_table::{IcebergLoadedTable, IcebergPhysicalTable};
use crate::typed_boundary::IcebergTypedBoundary;
use crate::typed_read::HiveTransactionHandle;
use novarocks_spi::connector::read_stack::{
    ConnectorPreparedSourceRead, ConnectorReadDynamicFilterSnapshot, ConnectorReadRequestControl,
    ConnectorReadSourceProjection, ConnectorReadSplitOpenRequest, ConnectorReadSplitPreparation,
    ConnectorReadSplitSource, ConnectorSession, TupleDomain,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorOriginalResultScope, ConnectorProcessLocalPayload,
    ConnectorTableObjectBindingFailure, ProviderMetadataCpuInput, ProviderMetadataCpuPort,
    ProviderMetadataCpuSource,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// A controllable external catalog response, on the ACTUAL load callback.
/// Other catalog behavior keeps the original formally admitted fixture owner.
struct ObservationCatalog {
    inner: Arc<dyn NovaRocksCatalog>,
    replacement: Mutex<Option<crate::iceberg::spec::TableMetadataRef>>,
    reads: Mutex<Vec<SnapshotLoading>>,
    runtime: crate::iceberg::Runtime,
}
impl std::fmt::Debug for ObservationCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparedSplitObservationCatalog")
    }
}
#[async_trait::async_trait]
impl NovaRocksCatalog for ObservationCatalog {
    fn implementation_name(&self) -> &'static str {
        "prepared-split-observation"
    }
    fn listing_admission(&self) -> Arc<crate::catalog::listing_admission::ListingAdmission> {
        self.inner.listing_admission()
    }
    async fn list_namespaces(
        &self,
        bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.inner.list_namespaces(bound).await
    }
    async fn namespace_exists(&self, name: CatalogNamespaceName) -> Result<bool, ConnectorError> {
        self.inner.namespace_exists(name).await
    }
    async fn list_tables(
        &self,
        name: CatalogNamespaceName,
        bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.inner.list_tables(name, bound).await
    }
    async fn table_exists(&self, name: CatalogTableName) -> Result<bool, ConnectorError> {
        self.inner.table_exists(name).await
    }
    async fn load_table(
        &self,
        name: CatalogTableName,
    ) -> Result<IcebergLoadedTable, ConnectorError> {
        self.inner.load_table(name).await
    }
    async fn load_table_for_read_with_snapshots(
        &self,
        name: CatalogTableName,
        binding: IcebergReadBinding,
        snapshots: SnapshotLoading,
    ) -> Result<IcebergLoadedTable, ConnectorError> {
        self.reads.lock().unwrap().push(snapshots);
        let loaded = self
            .inner
            .load_table_for_read_with_snapshots(name, binding, snapshots)
            .await?;
        let selected = self.replacement.lock().unwrap().clone();
        let Some(selected) = selected else {
            return Ok(loaded);
        };
        let (materialization, delegation) = loaded.into_parts();
        // The formal provider() fixture is static Hadoop. Do not invent a
        // credential fallback for a REST-deferred response.
        let original = materialization.into_static_table()?;
        let mut builder = crate::iceberg::table::Table::builder()
            .identifier(original.identifier().clone())
            .runtime(self.runtime.clone())
            .readonly(original.readonly())
            .file_io(original.file_io().clone())
            .metadata(selected);
        if let Some(location) = original.metadata_location() {
            builder = builder.metadata_location(location.to_string());
        }
        Ok(IcebergLoadedTable::new(
            builder.build().unwrap(),
            delegation,
        ))
    }
    async fn view_exists(&self, name: CatalogTableName) -> Result<bool, ConnectorError> {
        self.inner.view_exists(name).await
    }
    async fn list_views(
        &self,
        name: CatalogNamespaceName,
        bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.inner.list_views(name, bound).await
    }
    async fn load_view(
        &self,
        name: CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        self.inner.load_view(name).await
    }
    async fn anchor_written_metadata(
        &self,
        name: CatalogTableName,
        location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        self.inner.anchor_written_metadata(name, location).await
    }
    fn admit_operation(
        &self,
        operation: &crate::catalog::admission::CatalogOperation,
        target: &crate::catalog::admission::CatalogAdmissionTarget,
    ) -> Result<(), CatalogUnsupported> {
        self.inner.admit_operation(operation, target)
    }
}

struct Fixture {
    provider: IcebergMetadata,
    catalog: Arc<ObservationCatalog>,
    io: Arc<IoFixture>,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
    cpu_calls: Arc<PhasePort>,
    original: ConnectorOriginalResultScope,
    read: ConnectorReadRequestControl,
    prepared: ConnectorPreparedSourceRead,
    session: ConnectorSession,
    physical: IcebergPhysicalTable,
    setup_io: crate::iceberg::io::FileIO,
    from: i64,
    to: i64,
}
fn session() -> ConnectorSession {
    ConnectorSession::try_new("q", "u", "UTC", "en_US", std::time::SystemTime::UNIX_EPOCH).unwrap()
}
async fn fixture(original: &IcebergMetadata) -> Fixture {
    source_fixture(original, false).await
}
async fn source_fixture(original: &IcebergMetadata, snapshot_only: bool) -> Fixture {
    source_fixture_selection(original, snapshot_only, None).await
}
#[derive(Clone, Copy)]
enum EmptyWindow {
    SameSnapshot,
    DistinctSnapshots,
}
async fn source_fixture_selection(
    original: &IcebergMetadata,
    snapshot_only: bool,
    metadata_only: Option<EmptyWindow>,
) -> Fixture {
    assert!(!(snapshot_only && metadata_only.is_some()));
    for request in [
        CatalogPublicationRequest::CreateNamespace {
            namespace: CatalogNamespaceName::new("prepared_execution"),
            properties: HashMap::new(),
            no_op_if_exists: false,
        },
        CatalogPublicationRequest::CreateTable {
            namespace: CatalogNamespaceName::new("prepared_execution"),
            creation: crate::iceberg::TableCreation::builder()
                .name("t".into())
                .schema(
                    Schema::builder()
                        .with_fields(vec![Arc::new(NestedField::required(
                            1,
                            "id",
                            Type::Primitive(PrimitiveType::Long),
                        ))])
                        .build()
                        .unwrap(),
                )
                .format_version(FormatVersion::V2)
                .build(),
            no_op_if_exists: false,
        },
    ] {
        let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
            original.runtime(),
            original.incarnation,
            request,
        )
        .await;
        assert!(
            matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
            "{outcome:?}"
        );
    }
    let mut snapshots = Vec::new();
    for label in ["a", "b"] {
        original
            .runtime()
            .control_state()
            .invalidate_table_cache("prepared_execution", "t");
        let loaded = original
            .runtime()
            .load_table_for_request_async("prepared_execution", "t", &context())
            .await
            .unwrap();
        let files = if label == "b" && matches!(metadata_only, Some(EmptyWindow::DistinctSnapshots))
        {
            // The original staging helper requests a real SnapshotProducing
            // append. No new files leaves equal endpoint content, not equal IDs.
            Vec::new()
        } else {
            vec![written(&loaded.table, label)]
        };
        snapshots.push(
            crate::commit::run::append_snapshot_for_test(
                original.clone(),
                loaded.table,
                files,
                "main".into(),
                std::collections::BTreeMap::new(),
            )
            .await
            .unwrap(),
        );
    }
    // The original fixture's generation FileIO is retained only for test
    // corruption after preparation. Do not ask the read-only IoFixture to
    // open a writer stream or weaken its existing stream-refusal assertion.
    let (materialization, _) = original
        .runtime
        .novarocks_catalog()
        .load_table(CatalogTableName::new("prepared_execution", "t"))
        .await
        .unwrap()
        .into_parts();
    let setup_io = materialization
        .into_static_table()
        .unwrap()
        .file_io()
        .clone();
    let catalog = Arc::new(ObservationCatalog {
        inner: original.runtime.novarocks_catalog().clone(),
        replacement: Mutex::new(None),
        reads: Mutex::new(Vec::new()),
        runtime: original
            .runtime
            .resources()
            .catalog_runtime()
            .iceberg_runtime()
            .clone(),
    });
    let runtime = Arc::new(IcebergMetadataContext::with_catalog_for_test(
        original.runtime.control_state().clone(),
        original.runtime.resources().clone(),
        catalog.clone(),
    ));
    let provider = IcebergMetadata::new(original.descriptor.clone(), original.incarnation, runtime);
    let active = Arc::new(AtomicBool::new(false));
    let io = Arc::new(IoFixture {
        in_cpu: active.clone(),
        owned_reads: AtomicUsize::new(0),
    });
    let phase = Arc::new(PhasePort {
        in_cpu: active,
        descriptor: provider.descriptor.clone(),
        incarnation: provider.incarnation,
        calls: AtomicUsize::new(0),
    });
    let cpu: Arc<dyn ProviderMetadataCpuPort> = phase.clone();
    let context = context().with_io_admission(io.clone());
    let scope =
        ConnectorOriginalResultScope::from_original(Arc::new(OriginalScope(context.deadline())));
    let observed =
        prepared_source_fixture_metadata(&provider, "prepared_execution", &context).await;
    let request = prepared_source_fixture_window(
        &provider,
        &observed,
        "prepared_execution",
        snapshots[0],
        snapshots[1],
    );
    let request = if matches!(metadata_only, Some(EmptyWindow::SameSnapshot)) {
        novarocks_spi::connector::read_stack::ConnectorReadSourcePreparationRequest::change_window(
            request.name().clone(),
            request.observed_table().clone(),
            request.observed_source_facts().clone(),
            request.object_id().clone(),
            novarocks_spi::connector::read_stack::ConnectorReadChangeWindow::new(
                snapshots[1],
                snapshots[1],
            ),
            request.to_revision().clone(),
            request.to_revision().clone(),
        )
    } else if snapshot_only {
        novarocks_spi::connector::read_stack::ConnectorReadSourcePreparationRequest::snapshot(
            request.name().clone(),
            request.observed_table().clone(),
            request.observed_source_facts().clone(),
            request.object_id().clone(),
            snapshots[1],
            request.to_revision().clone(),
        )
    } else {
        request
    };
    let facts = request.observed_source_facts().value::<
        crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::IcebergObservedSourceReadFacts
    >(provider.descriptor.instance_id.as_str()).unwrap();
    let physical = facts
        .access
        .reacquired_request_scoped(
            provider
                .runtime
                .resources()
                .planning_binding()
                .for_request(context.clone()),
            provider
                .runtime
                .resources()
                .catalog_runtime()
                .iceberg_runtime()
                .clone(),
        )
        .unwrap();
    let read = prepared_source_fixture_control(&provider, &context);
    let metadata = read.metadata();
    let session = session();
    let mut owner = None;
    let prepared = metadata
        .prepare_source_read(&session, &request, &cpu, &scope, &mut owner)
        .await
        .unwrap();
    if snapshot_only {
        assert!(prepared.admission().is_none());
        assert!(prepared.request().from_revision().is_none());
    } else if metadata_only.is_some() {
        assert!(matches!(
            prepared.admission(),
            Some(novarocks_spi::connector::ConnectorChangeWindowAdmission::MetadataOnly)
        ));
    } else {
        assert!(matches!(
            prepared.admission(),
            Some(novarocks_spi::connector::ConnectorChangeWindowAdmission::Incremental { .. })
        ));
    }
    std::future::poll_fn(|cx| cpu.poll_closed(cx))
        .await
        .unwrap();
    drop(owner);
    catalog.reads.lock().unwrap().clear();
    Fixture {
        provider,
        catalog,
        io,
        cpu,
        cpu_calls: phase,
        original: scope,
        read,
        prepared,
        session,
        physical,
        setup_io,
        from: if matches!(metadata_only, Some(EmptyWindow::SameSnapshot)) {
            snapshots[1]
        } else {
            snapshots[0]
        },
        to: snapshots[1],
    }
}
impl Fixture {
    fn observe(&self, edit: impl FnOnce(&mut serde_json::Value)) {
        let mut value = serde_json::to_value(self.physical.table.metadata()).unwrap();
        edit(&mut value);
        *self.catalog.replacement.lock().unwrap() =
            Some(Arc::new(serde_json::from_value(value).unwrap()));
    }
    fn table(
        &self,
        projection: ConnectorReadSourceProjection,
    ) -> novarocks_spi::connector::read_stack::ConnectorReadTableHandle {
        self.read
            .metadata()
            .project_prepared_source_read(&self.session, &self.prepared, projection)
            .unwrap()
    }
    fn opening(
        &self,
        projection: ConnectorReadSourceProjection,
        cpu: Arc<dyn ProviderMetadataCpuPort>,
        owner: &mut Option<ConnectorProcessLocalPayload>,
    ) -> novarocks_spi::connector::read_stack::ConnectorReadSplitOpenFuture {
        let table = self.table(projection);
        self.read.splits().get_splits(
            ConnectorReadSplitOpenRequest::new(
                self.session.clone(),
                table,
                Vec::new(),
                std::collections::BTreeSet::new(),
                novarocks_spi::connector::read_stack::ConnectorReadConstraint::of_summary(
                    TupleDomain::all(),
                ),
            )
            .with_source_preparation(ConnectorReadSplitPreparation::new(
                cpu,
                self.original.clone(),
            )),
            owner,
        )
    }
    fn assert_actual_all(&self) {
        assert_eq!(
            *self.catalog.reads.lock().unwrap(),
            vec![SnapshotLoading::All],
            "one actual catalog callback; not static access reconstruction or a cache hit"
        );
    }
    fn adapter(
        &self,
    ) -> novarocks_spi::connector::read_stack::adapter::ReadRuntimeAdapter<IcebergTypedBoundary>
    {
        Arc::new(IcebergTypedBoundary::new(
            self.provider.descriptor.clone(),
            self.provider.incarnation,
            CatalogHandle::new(
                self.provider.descriptor.instance_id.clone(),
                CatalogVersion::from_bytes([0x65; 32]),
            ),
            HiveTransactionHandle::new(true, [0x71; 16]),
            self.provider.runtime.clone(),
        ))
        .read_runtime_adapter()
    }
    fn paths(&self, source: &mut dyn ConnectorReadSplitSource) -> Vec<String> {
        let adapter = self.adapter();
        let mut paths = Vec::new();
        loop {
            let batch = source
                .next_batch(64, &ConnectorReadDynamicFilterSnapshot::all_complete())
                .unwrap();
            let finished = batch.no_more_splits();
            for split in batch.into_splits() {
                let crate::typed_read::IcebergReadSplit::Data(data) =
                    adapter.split(&split).unwrap()
                else {
                    panic!("To Table must retain its data split kind");
                };
                assert_eq!(data.read_domain().endpoint().snapshot_id(), self.to);
                paths.push(data.path().to_string());
            }
            if finished {
                break;
            }
        }
        paths.sort_unstable();
        paths
    }
    async fn close_source(
        &self,
        mut source: Box<dyn ConnectorReadSplitSource>,
        mut owner: Option<ConnectorProcessLocalPayload>,
        cpu: &Arc<dyn ProviderMetadataCpuPort>,
    ) {
        source.close().unwrap();
        drop(source);
        std::future::poll_fn(|cx| cpu.poll_closed(cx))
            .await
            .unwrap();
        drop(owner.take());
    }
    async fn corrupt_from_list(&self) {
        let path = self
            .physical
            .table
            .metadata()
            .snapshot_by_id(self.from)
            .unwrap()
            .manifest_list();
        let mut output = self
            .setup_io
            .new_output(path)
            .unwrap()
            .writer()
            .await
            .unwrap();
        output
            .write(Bytes::from_static(
                b"invalid original From manifest list avro",
            ))
            .await
            .unwrap();
        output.close().await.unwrap();
    }
}
fn snapshot_mut(value: &mut serde_json::Value, id: i64) -> &mut serde_json::Value {
    value["snapshots"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|snapshot| snapshot["snapshot-id"].as_i64() == Some(id))
        .unwrap()
}

#[test]
fn actual_metadata_only_delta_is_empty_without_observation_but_to_still_reads() {
    for selection in [EmptyWindow::SameSnapshot, EmptyWindow::DistinctSnapshots] {
        let (executor, _warehouse, provider) = provider();
        executor.block_on(async {
            let fixture = source_fixture_selection(&provider, false, Some(selection)).await;
            assert_eq!(
                fixture.from == fixture.to,
                matches!(selection, EmptyWindow::SameSnapshot)
            );
            assert!(
                fixture
                    .read
                    .metadata()
                    .project_prepared_source_read(
                        &fixture.session,
                        &fixture.prepared,
                        ConnectorReadSourceProjection::From,
                    )
                    .is_err()
            );
            let before_io = fixture.io.owned_reads.load(Ordering::SeqCst);
            let before_cpu = fixture.cpu_calls.calls.load(Ordering::SeqCst);
            let mut owner = None;
            let mut delta = fixture
                .opening(
                    ConnectorReadSourceProjection::Delta,
                    fixture.cpu.clone(),
                    &mut owner,
                )
                .await
                .unwrap();
            assert!(
                owner.is_none(),
                "closed empty Delta must not create an observation owner"
            );
            let batch = delta
                .next_batch(64, &ConnectorReadDynamicFilterSnapshot::all_complete())
                .unwrap();
            assert!(batch.no_more_splits());
            assert!(batch.into_splits().is_empty());
            assert!(delta.is_finished());
            delta.close().unwrap();
            drop(delta);
            assert_eq!(fixture.io.owned_reads.load(Ordering::SeqCst), before_io);
            assert_eq!(fixture.cpu_calls.calls.load(Ordering::SeqCst), before_cpu);
            assert!(
                fixture.catalog.reads.lock().unwrap().is_empty(),
                "empty Delta must not issue fresh catalog observation"
            );
            let mut to_owner = None;
            let mut to = fixture
                .opening(
                    ConnectorReadSourceProjection::To,
                    fixture.cpu.clone(),
                    &mut to_owner,
                )
                .await
                .unwrap();
            assert!(
                to_owner.is_some(),
                "MetadataOnly must not globally suppress a real To read"
            );
            fixture.assert_actual_all();
            assert_eq!(
                fixture.paths(to.as_mut()).len(),
                if matches!(selection, EmptyWindow::SameSnapshot) {
                    2
                } else {
                    1
                }
            );
            assert!(fixture.io.owned_reads.load(Ordering::SeqCst) > before_io);
            assert!(fixture.cpu_calls.calls.load(Ordering::SeqCst) > before_cpu);
            fixture.close_source(to, to_owner, &fixture.cpu).await;
        });
    }
}

#[derive(Debug)]
struct InactiveOpeningControl;
impl std::fmt::Display for InactiveOpeningControl {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("the original opening control cause must not be formatted")
    }
}
impl std::error::Error for InactiveOpeningControl {}
impl novarocks_failure_contract::FailureProjection for InactiveOpeningControl {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(
            &ConnectorErrorKind::Interrupted,
        )
    }
}
struct InactiveOpeningScope {
    deadline: Instant,
    calls: AtomicUsize,
}
impl novarocks_spi::connector::ConnectorOriginalResultScopeCheck for InactiveOpeningScope {
    fn original_deadline(&self) -> Instant {
        self.deadline
    }
    fn check_active(&self) -> Result<(), novarocks_spi::connector::OriginalResultCheckError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(novarocks_spi::connector::OriginalResultCheckError::new(
            novarocks_spi::connector::OriginalResultCheckClass::Control,
            InactiveOpeningControl,
        ))
    }
}

#[test]
fn actual_metadata_only_delta_preserves_inactive_original_scope_before_empty_success() {
    use novarocks_failure_contract::FailureProjection;
    for selection in [EmptyWindow::SameSnapshot, EmptyWindow::DistinctSnapshots] {
        let (executor, _warehouse, provider) = provider();
        executor.block_on(async {
            let mut fixture = source_fixture_selection(&provider, false, Some(selection)).await;
            let original = Arc::new(InactiveOpeningScope {
                deadline: fixture.original.original_deadline(),
                calls: AtomicUsize::new(0),
            });
            fixture.original = ConnectorOriginalResultScope::from_original(original.clone());
            // Successful projection still checks the actual Provider context.
            // Only the independent original Root scope refuses this opening.
            let _ = fixture.table(ConnectorReadSourceProjection::Delta);
            let before_io = fixture.io.owned_reads.load(Ordering::SeqCst);
            let before_cpu = fixture.cpu_calls.calls.load(Ordering::SeqCst);
            let mut owner = None;
            let result = fixture
                .opening(
                    ConnectorReadSourceProjection::Delta,
                    fixture.cpu.clone(),
                    &mut owner,
                )
                .await;
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("a closed empty Delta cannot bypass its original Root refusal"),
            };
            assert_eq!(error.kind(), ConnectorErrorKind::Interrupted);
            assert_eq!(
                error.failure_fact(),
                ConnectorErrorKind::Interrupted.failure_fact()
            );
            let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
            let mut original_cause_retained = false;
            while let Some(current) = cause {
                original_cause_retained |= current.is::<InactiveOpeningControl>();
                cause = current.source();
            }
            assert!(original_cause_retained);
            assert_eq!(original.calls.load(Ordering::SeqCst), 1);
            assert!(owner.is_none());
            assert_eq!(fixture.io.owned_reads.load(Ordering::SeqCst), before_io);
            assert_eq!(fixture.cpu_calls.calls.load(Ordering::SeqCst), before_cpu);
            assert!(fixture.catalog.reads.lock().unwrap().is_empty());
        });
    }
}

#[test]
fn actual_prepared_to_observes_later_append_but_reads_the_original_to_files() {
    let (executor, _warehouse, provider) = provider();
    executor.block_on(async {
        let fixture = fixture(&provider).await;
        fixture.observe(|value| {
            let next_id = fixture.to.checked_add(1).unwrap();
            let mut schema = value["schemas"][0].clone();
            let schema_id = schema["schema-id"].as_i64().unwrap() + 1;
            schema["schema-id"] = serde_json::json!(schema_id);
            schema["fields"][0]["name"] = serde_json::json!("future_name");
            value["schemas"].as_array_mut().unwrap().push(schema);
            value["current-schema-id"] = serde_json::json!(schema_id);
            let mut next = snapshot_mut(value, fixture.to).clone();
            next["snapshot-id"] = serde_json::json!(next_id);
            next["parent-snapshot-id"] = serde_json::json!(fixture.to);
            next["sequence-number"] =
                serde_json::json!(value["last-sequence-number"].as_i64().unwrap() + 1);
            next["schema-id"] = serde_json::json!(schema_id);
            next["manifest-list"] = serde_json::json!("file:///future-unread-manifest-list.avro");
            value["last-sequence-number"] = next["sequence-number"].clone();
            value["snapshots"].as_array_mut().unwrap().push(next);
            value["current-snapshot-id"] = serde_json::json!(next_id);
            value["refs"]["main"]["snapshot-id"] = serde_json::json!(next_id);
            let mut spec = value["partition-specs"][0].clone();
            spec["spec-id"] = serde_json::json!(1);
            value["partition-specs"].as_array_mut().unwrap().push(spec);
            value["default-spec-id"] = serde_json::json!(1);
            value["location"] = serde_json::json!(format!(
                "{}/future",
                fixture.physical.table.metadata().location()
            ));
            value["properties"]["unrelated.future"] = serde_json::json!("changed");
        });
        let baseline = fixture.io.owned_reads.load(Ordering::SeqCst);
        let mut owner = None;
        let mut source = fixture
            .opening(
                ConnectorReadSourceProjection::To,
                fixture.cpu.clone(),
                &mut owner,
            )
            .await
            .unwrap();
        fixture.assert_actual_all();
        let paths = fixture.paths(source.as_mut());
        assert_eq!(paths.len(), 2);
        assert!(paths[0].ends_with("/a.parquet"));
        assert!(paths[1].ends_with("/b.parquet"));
        assert!(
            fixture.io.owned_reads.load(Ordering::SeqCst) > baseline,
            "original MIO actually serviced frozen Avro discovery/spec-slot backing"
        );
        fixture.close_source(source, owner, &fixture.cpu).await;
    });
}

#[test]
fn actual_snapshot_to_without_from_reads_its_frozen_table_after_later_append() {
    let (executor, _warehouse, provider) = provider();
    executor.block_on(async {
        let fixture = source_fixture(&provider, true).await;
        assert!(fixture.prepared.request().from_revision().is_none());
        assert!(matches!(
            fixture.prepared.request().selection(),
            novarocks_spi::connector::read_stack::ConnectorReadSourceSelection::Snapshot(id)
                if id == fixture.to
        ));
        fixture.observe(|value| {
            let next_id = fixture.to.checked_add(1).unwrap();
            let mut next = snapshot_mut(value, fixture.to).clone();
            next["snapshot-id"] = serde_json::json!(next_id);
            next["parent-snapshot-id"] = serde_json::json!(fixture.to);
            next["sequence-number"] =
                serde_json::json!(value["last-sequence-number"].as_i64().unwrap() + 1);
            next["manifest-list"] = serde_json::json!("file:///later-unread-manifest-list.avro");
            value["last-sequence-number"] = next["sequence-number"].clone();
            value["snapshots"].as_array_mut().unwrap().push(next);
            value["current-snapshot-id"] = serde_json::json!(next_id);
            value["refs"]["main"]["snapshot-id"] = serde_json::json!(next_id);
        });
        assert!(
            fixture
                .read
                .seal_attempt_access(&fixture.table(ConnectorReadSourceProjection::To))
                .unwrap()
                .requires_metadata_split_preparation()
        );
        let baseline = fixture.io.owned_reads.load(Ordering::SeqCst);
        let mut owner = None;
        let mut source = fixture
            .opening(
                ConnectorReadSourceProjection::To,
                fixture.cpu.clone(),
                &mut owner,
            )
            .await
            .unwrap();
        fixture.assert_actual_all();
        let paths = fixture.paths(source.as_mut());
        assert_eq!(paths.len(), 2);
        assert!(paths[0].ends_with("/a.parquet"));
        assert!(paths[1].ends_with("/b.parquet"));
        assert!(fixture.io.owned_reads.load(Ordering::SeqCst) > baseline);
        fixture.close_source(source, owner, &fixture.cpu).await;
    });
}

#[test]
fn actual_prepared_split_rejects_replaced_uuid_from_the_fresh_catalog_callback() {
    let (executor, _warehouse, provider) = provider();
    executor.block_on(async {
        let fixture = fixture(&provider).await;
        fixture.observe(|value| value["table-uuid"] = serde_json::json!(uuid::Uuid::new_v4()));
        let mut owner = None;
        let error = match fixture
            .opening(
                ConnectorReadSourceProjection::Delta,
                fixture.cpu.clone(),
                &mut owner,
            )
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("fresh replaced object was accepted"),
        };
        assert_eq!(
            error.table_object_binding_failure(),
            Some(ConnectorTableObjectBindingFailure::Replaced)
        );
        fixture.assert_actual_all();
        assert!(owner.is_some());
        std::future::poll_fn(|cx| fixture.cpu.poll_closed(cx))
            .await
            .unwrap();
        drop(owner);
    });
}

#[test]
fn actual_from_avro_failure_wins_over_to_fresh_descriptor_drift() {
    let (executor, _warehouse, provider) = provider();
    executor.block_on(async {
        let fixture = fixture(&provider).await;
        fixture.observe(|value| {
            snapshot_mut(value, fixture.to)["manifest-list"] =
                serde_json::json!("file:///wrong-To-descriptor.avro")
        });
        // First establish that this exact public path rejects the To-only
        // proof fault while From is still complete. Both endpoints' raw FileIO
        // errors share a safe public detail, so that text alone is not origin.
        let mut proof_owner = None;
        let proof_error = match fixture
            .opening(
                ConnectorReadSourceProjection::Delta,
                fixture.cpu.clone(),
                &mut proof_owner,
            )
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("fresh To descriptor drift was accepted"),
        };
        assert_eq!(proof_error.kind(), ConnectorErrorKind::CorruptData);
        assert_eq!(
            proof_error.message(),
            "Iceberg prepared endpoint descriptor changed"
        );
        fixture.assert_actual_all();
        std::future::poll_fn(|cx| fixture.cpu.poll_closed(cx))
            .await
            .unwrap();
        drop(proof_owner);
        fixture.catalog.reads.lock().unwrap().clear();
        // With the same To proof fault still present, the original From Avro
        // failure must now win; the second open also observes exactly All once.
        fixture.corrupt_from_list().await;
        let mut owner = None;
        let error = match fixture
            .opening(
                ConnectorReadSourceProjection::Delta,
                fixture.cpu.clone(),
                &mut owner,
            )
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("two invalid endpoints were accepted"),
        };
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert_eq!(
            error.message(),
            "original snapshot metadata I/O failed",
            "To descriptor failure must not hide the real From Avro failure"
        );
        fixture.assert_actual_all();
        std::future::poll_fn(|cx| fixture.cpu.poll_closed(cx))
            .await
            .unwrap();
        drop(owner);
    });
}
#[test]
fn actual_to_only_ignores_historical_from_proof_and_manifest_list() {
    let (executor, _warehouse, provider) = provider();
    executor.block_on(async {
        let fixture = fixture(&provider).await;
        fixture.corrupt_from_list().await;
        fixture.observe(|value| {
            snapshot_mut(value, fixture.from)["manifest-list"] =
                serde_json::json!("file:///changed-historical-From.avro")
        });
        let mut owner = None;
        let mut source = fixture
            .opening(
                ConnectorReadSourceProjection::To,
                fixture.cpu.clone(),
                &mut owner,
            )
            .await
            .unwrap();
        fixture.assert_actual_all();
        assert_eq!(fixture.paths(source.as_mut()).len(), 2);
        fixture.close_source(source, owner, &fixture.cpu).await;
    });
}

struct HeldPhasePort {
    inner: PhasePort,
    released: AtomicBool,
    original: Mutex<Option<Arc<dyn ProviderMetadataCpuSource>>>,
    wake: futures::task::AtomicWaker,
}
impl ProviderMetadataCpuPort for HeldPhasePort {
    fn submit_metadata_in(
        &self,
        input: &mut Option<ProviderMetadataCpuInput>,
    ) -> Result<(), ConnectorError> {
        if self.inner.calls.load(Ordering::SeqCst) == 0 {
            *self.original.lock().unwrap() = Some(input.as_ref().unwrap().0.clone());
        }
        self.inner.submit_metadata_in(input)
    }
    fn poll_finished(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.wake.register(cx.waker());
        if !self.released.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        drop(self.original.lock().unwrap().take());
        Poll::Ready(Ok(()))
    }
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.poll_finished(cx)
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        None
    }
}
#[test]
fn held_prepared_split_cpu_footer_keeps_original_owner_across_borrowed_waiter_drop() {
    let (executor, _warehouse, provider) = provider();
    executor.block_on(async {
        let fixture = fixture(&provider).await;
        let held = Arc::new(HeldPhasePort {
            inner: PhasePort {
                in_cpu: fixture.io.in_cpu.clone(),
                descriptor: fixture.provider.descriptor.clone(),
                incarnation: fixture.provider.incarnation,
                calls: AtomicUsize::new(0),
            },
            released: AtomicBool::new(false),
            original: Mutex::new(None),
            wake: futures::task::AtomicWaker::new(),
        });
        let cpu: Arc<dyn ProviderMetadataCpuPort> = held.clone();
        let mut owner = None;
        let mut opening =
            fixture.opening(ConnectorReadSourceProjection::To, cpu.clone(), &mut owner);
        let original = owner
            .as_ref()
            .expect("Source is installed synchronously before driver polling")
            .value::<crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal>(
                fixture.provider.descriptor.instance_id.as_str(),
            )
            .unwrap();
        let weak = Arc::downgrade(original);
        {
            let borrowed = std::future::poll_fn(|cx| {
                assert!(opening.as_mut().poll(cx).is_pending());
                if held.inner.calls.load(Ordering::SeqCst) > 0 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            });
            tokio::time::timeout(Duration::from_secs(5), borrowed)
                .await
                .unwrap();
        }
        assert!(weak.upgrade().is_some());
        assert!(owner.is_some());
        std::future::poll_fn(|cx| {
            assert!(cpu.poll_closed(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(
            weak.upgrade().is_some(),
            "a pending component footer cannot retire its actual Source"
        );
        held.released.store(true, Ordering::SeqCst);
        held.wake.wake();
        let mut source = tokio::time::timeout(Duration::from_secs(5), opening.as_mut())
            .await
            .unwrap()
            .unwrap();
        drop(opening);
        fixture.assert_actual_all();
        assert_eq!(fixture.paths(source.as_mut()).len(), 2);
        fixture.close_source(source, owner, &cpu).await;
        assert!(
            weak.upgrade().is_none(),
            "no runtime Source alias is hidden in final split facts"
        );
    });
}

/// This is the real vended REST owner, not a static Table substituted by a
/// callback. UUID rejection must happen before opening any S3 manifest.
#[test]
fn actual_vended_prepared_source_keeps_renewal_and_rejects_fresh_replaced_uuid() {
    vended_prepared_source_fresh_refusal(false);
}

#[test]
fn actual_vended_prepared_source_rejects_fresh_missing_endpoint_without_manifest_io() {
    vended_prepared_source_fresh_refusal(true);
}

fn vended_prepared_source_fresh_refusal(expire_selected_endpoint: bool) {
    use crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::IcebergObservedSourceReadFacts;
    use crate::catalog_runtime::RestAccessDelegationMode;
    use crate::iceberg::spec::UnboundPartitionSpec;
    use crate::loaded_table::IcebergVendedS3RenewalCapability;
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};
    use novarocks_spi::connector::read_stack::{
        ConnectorReadSourcePreparationRequest, SchemaTableName,
    };

    let metadata = TableMetadataBuilder::new(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap(),
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        "s3://warehouse/db/t".into(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let original_uuid = metadata.uuid();
    let mut wire = serde_json::to_value(&metadata).unwrap();
    let timestamp = wire["last-updated-ms"].clone();
    wire["last-sequence-number"] = serde_json::json!(1);
    wire["current-snapshot-id"] = serde_json::json!(22);
    wire["snapshots"] = serde_json::json!([{
        "snapshot-id": 22, "sequence-number": 1, "timestamp-ms": timestamp,
        "manifest-list": "s3://warehouse/db/t/metadata/list-22.avro",
        "summary": {"operation": "append", "total-records": "0"}, "schema-id": 0,
    }]);
    wire["snapshot-log"] = serde_json::json!([{"timestamp-ms": timestamp, "snapshot-id": 22}]);
    wire["refs"] = serde_json::json!({"main": {"snapshot-id": 22, "type": "branch"}});
    let expiry = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 3_600_000)
        .to_string();
    let response = serde_json::json!({
        "metadata-location": "s3://warehouse/db/t/metadata/v1.json", "metadata": wire,
        "storage-credentials": [{"prefix": "s3://warehouse/db/t", "config": {
            "s3.access-key-id": "prepared-vended-key-canary",
            "s3.secret-access-key": "prepared-vended-secret-canary",
            "s3.session-token": "prepared-vended-session-canary",
            "s3.session-token-expires-at-ms": expiry,
        }}],
    });
    let mut replaced = response.clone();
    if expire_selected_endpoint {
        replaced["metadata-location"] = serde_json::json!("s3://warehouse/db/t/metadata/v2.json");
        assert_ne!(replaced["metadata-location"], response["metadata-location"]);
        let mut next = replaced["metadata"]["snapshots"][0].clone();
        next["snapshot-id"] = serde_json::json!(23);
        next["parent-snapshot-id"] = serde_json::json!(22);
        next["sequence-number"] = serde_json::json!(2);
        next["manifest-list"] = serde_json::json!("s3://warehouse/db/t/metadata/list-23.avro");
        replaced["metadata"]["last-sequence-number"] = serde_json::json!(2);
        replaced["metadata"]["current-snapshot-id"] = serde_json::json!(23);
        replaced["metadata"]["snapshots"] = serde_json::json!([next]);
        replaced["metadata"]["snapshot-log"] =
            serde_json::json!([{"timestamp-ms": timestamp, "snapshot-id": 23}]);
        replaced["metadata"]["refs"]["main"]["snapshot-id"] = serde_json::json!(23);
        assert_eq!(
            replaced["metadata"]["table-uuid"],
            response["metadata"]["table-uuid"]
        );
        assert!(
            replaced["metadata"]["snapshots"]
                .as_array()
                .unwrap()
                .iter()
                .all(|snapshot| snapshot["snapshot-id"].as_i64() != Some(22))
        );
    } else {
        let replaced_uuid = uuid::Uuid::new_v4();
        assert_ne!(original_uuid, replaced_uuid);
        replaced["metadata"]["table-uuid"] = serde_json::json!(replaced_uuid);
    }
    let server = TestServer::start(
        Script::new()
            .on("GET", "/v1/config", Reply::config(&[], &[], None))
            .on_sequence(
                "GET",
                "/v1/namespaces/db/tables/t",
                vec![Reply::json(200, &response), Reply::json(200, &replaced)],
            ),
    );
    let executor = tokio::runtime::Runtime::new().unwrap();
    let configuration = crate::catalog_config::parse_catalog_configuration(
        "ice",
        &[
            ("iceberg.catalog.type".into(), "rest".into()),
            ("iceberg.catalog.warehouse".into(), "s3://warehouse".into()),
            ("iceberg.catalog.uri".into(), server.url()),
        ],
    )
    .unwrap();
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(executor.handle().clone())),
        Arc::new(TokioFileTaskSpawner::new(executor.handle().clone())),
    );
    let runtime = Arc::new(
        IcebergMetadataContext::try_new_with_rest_access_delegation(
            IcebergCatalogControlState::new(configuration),
            IcebergMetadataResources::new(binding, executor.handle().clone()),
            RestAccessDelegationMode::Vended,
        )
        .unwrap(),
    );
    let provider = IcebergMetadata::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
        },
        ProviderBindingEpoch::from_bytes([7; 16]),
        runtime,
    );
    executor.block_on(async {
        let active = Arc::new(AtomicBool::new(false));
        let io = Arc::new(IoFixture {
            in_cpu: active.clone(),
            owned_reads: AtomicUsize::new(0),
        });
        let cpu: Arc<dyn ProviderMetadataCpuPort> = Arc::new(PhasePort {
            in_cpu: active,
            descriptor: provider.descriptor.clone(),
            incarnation: provider.incarnation,
            calls: AtomicUsize::new(0),
        });
        let context = context().with_io_admission(io.clone());
        let original = ConnectorOriginalResultScope::from_original(Arc::new(OriginalScope(
            context.deadline(),
        )));
        let observed = prepared_source_fixture_metadata(&provider, "db", &context).await;
        let facts = observed
            .source_read_facts()
            .unwrap()
            .value::<IcebergObservedSourceReadFacts>(provider.descriptor.instance_id.as_str())
            .unwrap();
        assert_eq!(facts.snapshots, SnapshotLoading::All);
        assert_eq!(facts.access.metadata().uuid(), original_uuid);
        assert!(matches!(
            facts.access.renewal(),
            Some(IcebergVendedS3RenewalCapability::LoadTableDelegation(_))
        ));
        let object =
            ConnectorTableObjectId::try_new(Bytes::from(original_uuid.to_string())).unwrap();
        let revision = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            provider.descriptor.provider_id.clone(),
            &object,
            Some(22),
        )
        .unwrap();
        let request = ConnectorReadSourcePreparationRequest::snapshot(
            SchemaTableName::try_new("db", "t").unwrap(),
            observed.table.clone(),
            observed.source_read_facts().unwrap().clone(),
            object,
            22,
            revision,
        );
        let read = prepared_source_fixture_control(&provider, &context);
        let mut preparation_owner = None;
        let prepared = read
            .metadata()
            .prepare_source_read(
                &session(),
                &request,
                &cpu,
                &original,
                &mut preparation_owner,
            )
            .await
            .unwrap();
        assert!(
            prepared.admission().is_none(),
            "a first/full To source must not pretend to be Delta"
        );
        let selected = prepared
            .to_metadata()
            .source_read_facts()
            .unwrap()
            .value::<IcebergObservedSourceReadFacts>(provider.descriptor.instance_id.as_str())
            .unwrap();
        assert!(matches!(
            selected.access.renewal(),
            Some(IcebergVendedS3RenewalCapability::LoadTableDelegation(_))
        ));
        assert_eq!(
            selected.access.request_cache_identity(),
            facts.access.request_cache_identity()
        );
        for table in [&observed.table, &prepared.to_metadata().table] {
            let bytes = std::str::from_utf8(table.payload()).unwrap();
            for canary in [
                "prepared-vended-key-canary",
                "prepared-vended-secret-canary",
                "prepared-vended-session-canary",
            ] {
                assert!(
                    !bytes.contains(canary),
                    "vended response material is not a table attribute"
                );
            }
        }
        std::future::poll_fn(|cx| cpu.poll_closed(cx))
            .await
            .unwrap();
        drop(preparation_owner);
        assert_eq!(
            server.count("GET", "/v1/namespaces/db/tables/t"),
            1,
            "SnapshotTo preparation must use the frozen observation, not late Current reload"
        );
        let table = read
            .metadata()
            .project_prepared_source_read(&session(), &prepared, ConnectorReadSourceProjection::To)
            .unwrap();
        assert!(
            read.seal_attempt_access(&table)
                .unwrap()
                .requires_metadata_split_preparation()
        );
        let owned_before_fresh = io.owned_reads.load(Ordering::SeqCst);
        let mut owner = None;
        let error = match read
            .splits()
            .get_splits(
                ConnectorReadSplitOpenRequest::new(
                    session(),
                    table,
                    Vec::new(),
                    std::collections::BTreeSet::new(),
                    novarocks_spi::connector::read_stack::ConnectorReadConstraint::of_summary(
                        TupleDomain::all(),
                    ),
                )
                .with_source_preparation(ConnectorReadSplitPreparation::new(cpu.clone(), original)),
                &mut owner,
            )
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("vended fresh source invalidation was accepted"),
        };
        if expire_selected_endpoint {
            assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
            assert_eq!(error.table_object_binding_failure(), None);
            assert_eq!(
                error.message(),
                "Iceberg prepared endpoint is no longer visible"
            );
        } else {
            assert_eq!(
                error.table_object_binding_failure(),
                Some(ConnectorTableObjectBindingFailure::Replaced)
            );
        }
        assert_eq!(
            io.owned_reads.load(Ordering::SeqCst),
            owned_before_fresh,
            "fresh source refusal precedes original manifest read/spec-slot backing admission"
        );
        assert!(
            owner.is_some(),
            "the original source still owns its actual result/exit inventory"
        );
        std::future::poll_fn(|cx| cpu.poll_closed(cx))
            .await
            .unwrap();
        drop(owner);
        let loads = server.requests_to("GET", "/v1/namespaces/db/tables/t");
        assert_eq!(
            loads.len(),
            2,
            "one initial observation plus one real fresh observation"
        );
        for load in &loads {
            assert_eq!(
                load.query(),
                None,
                "All uses the original complete default load; never refs"
            );
            assert_eq!(
                load.header("x-iceberg-access-delegation"),
                Some("vended-credentials")
            );
        }
        assert_eq!(
            server.count("GET", "/v1/config"),
            1,
            "one original REST generation/session"
        );
        assert_eq!(
            server.count("GET", "/v1/namespaces/db/tables/t/credentials"),
            0,
            "the observed load-table renewal capability cannot turn into another acquisition path"
        );
        assert_eq!(
            server.requests().len(),
            3,
            "the real REST owner must not dispatch any guessed additional endpoint"
        );
    });
    // TestServer Drop joins every original loopback actor after the original
    // opening Source/CPU component has been explicitly closed above.
}
