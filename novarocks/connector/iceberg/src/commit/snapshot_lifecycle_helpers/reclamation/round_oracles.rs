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

//! Actual catalog and FileIO oracles for the bounded reclamation loop.

use super::*;
use crate::iceberg::io::{
    FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use crate::iceberg::spec::{
    DataFileBuilder, ManifestListWriter, ManifestWriterBuilder, Operation, SnapshotReference,
    SnapshotRetention, Struct, Summary,
};
use crate::opendal::raw::oio;
use crate::rest_protocol::test_server::{Reply, Script, TestServer};
use bytes::Bytes;
use futures::stream::BoxStream;
use novarocks_spi::connector::ConnectorStopOwner;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug)]
struct ManualClock {
    origin: Instant,
    ms: AtomicU64,
}
impl ManualClock {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
            ms: AtomicU64::new(0),
        }
    }
    fn advance(&self, ms: u64) {
        self.ms.fetch_add(ms, Ordering::SeqCst);
    }
    fn millis(&self) -> u64 {
        self.ms.load(Ordering::SeqCst)
    }
}
impl ReclamationClock for ManualClock {
    fn now(&self) -> Instant {
        self.origin + Duration::from_millis(self.millis())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct Event {
    kind: &'static str,
    path: String,
    ms: u64,
}
type ReadGate = (String, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);

struct Events {
    clock: Arc<ManualClock>,
    log: Mutex<Vec<Event>>,
    // One-shot, complete real reads/stat calls consume virtual time.
    read_advance: Mutex<BTreeMap<String, u64>>,
    stat_advance: Mutex<BTreeMap<String, u64>>,
    delete_advance: u64,
    data_only_time: bool,
    cancel_read: Mutex<Option<(String, ConnectorStopOwner)>>,
    delete_delay: Duration,
    actual_gc_deadline: Mutex<Option<Instant>>,
    read_gate: Mutex<Option<ReadGate>>,
    delete_access_started: tokio::sync::Notify,
}
impl std::fmt::Debug for Events {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Events").finish_non_exhaustive()
    }
}
impl Events {
    fn record(&self, kind: &'static str, path: &str) {
        self.log.lock().unwrap().push(Event {
            kind,
            path: path.to_owned(),
            ms: self.clock.millis(),
        });
    }
    fn count(&self, kind: &str) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.kind == kind)
            .count()
    }
}
macro_rules! local_only {
    ($ty:ty) => {
        impl serde::Serialize for $ty {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom(
                    "Test I/O capability is process local",
                ))
            }
        }
        impl<'de> serde::Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
                Err(serde::de::Error::custom(
                    "Test I/O capability is process local",
                ))
            }
        }
    };
}
#[derive(Debug, Clone)]
struct ReadStorage {
    original: FileIO,
    events: Arc<Events>,
}
#[derive(Debug)]
struct ReadFactory(ReadStorage);
local_only!(ReadStorage);
local_only!(ReadFactory);
#[typetag::serde(name = "IcebergReclamationRoundOracleFactory")]
impl StorageFactory for ReadFactory {
    fn build(&self, _: &StorageConfig) -> crate::iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.0.clone()))
    }
}
#[async_trait::async_trait]
#[typetag::serde(name = "IcebergReclamationRoundOracleStorage")]
impl Storage for ReadStorage {
    async fn exists(&self, p: &str) -> crate::iceberg::Result<bool> {
        self.original.new_input(p)?.exists().await
    }
    async fn metadata(&self, p: &str) -> crate::iceberg::Result<FileMetadata> {
        self.events.record("READ_HEAD", p);
        self.original.new_input(p)?.metadata().await
    }
    async fn read(&self, p: &str) -> crate::iceberg::Result<Bytes> {
        self.events.record("GET", p);
        let bytes = self.original.new_input(p)?.read().await?;
        let gate = {
            let mut gate = self.events.read_gate.lock().unwrap();
            if gate.as_ref().is_some_and(|(path, _, _)| path == p) {
                gate.take()
            } else {
                None
            }
        };
        if let Some((_, entered, resume)) = gate {
            entered.notify_one();
            resume.notified().await;
        }

        if let Some(ms) = self.events.read_advance.lock().unwrap().remove(p) {
            self.events.clock.advance(ms);
        }
        let cancel = {
            let mut c = self.events.cancel_read.lock().unwrap();
            if c.as_ref().is_some_and(|(path, _)| path == p) {
                c.take()
            } else {
                None
            }
        };
        if let Some((_, stop)) = cancel {
            stop.request_stop();
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::Unexpected,
                "Cancelled during actual manifest read",
            )
            .with_source(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "scan cancelled",
            )));
        }
        Ok(bytes)
    }
    async fn reader(&self, p: &str) -> crate::iceberg::Result<Box<dyn FileRead>> {
        self.events.record("RANGE_OPEN", p);
        self.original.new_input(p)?.reader().await
    }
    async fn write(&self, p: &str, b: Bytes) -> crate::iceberg::Result<()> {
        self.original.new_output(p)?.write(b).await
    }
    async fn writer(&self, p: &str) -> crate::iceberg::Result<Box<dyn FileWrite>> {
        self.original.new_output(p)?.writer().await
    }
    async fn delete(&self, p: &str) -> crate::iceberg::Result<()> {
        self.original.delete(p).await
    }
    async fn delete_prefix(&self, p: &str) -> crate::iceberg::Result<()> {
        self.original.delete_prefix(p).await
    }
    async fn delete_stream(&self, p: BoxStream<'static, String>) -> crate::iceberg::Result<()> {
        self.original.delete_stream(p).await
    }
    fn new_input(&self, p: &str) -> crate::iceberg::Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), p.to_owned()))
    }
    fn new_output(&self, p: &str) -> crate::iceberg::Result<OutputFile> {
        self.original.new_output(p)
    }
}
#[derive(Debug, Clone)]
struct DeleteAccess {
    inner: crate::opendal::raw::Servicer,
    events: Arc<Events>,
}
impl crate::opendal::raw::Service for DeleteAccess {
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type Deleter = crate::opendal::raw::oio::Deleter;
    type Copier = ();
    fn info(&self) -> crate::opendal::raw::ServiceInfo {
        crate::opendal::raw::Service::info(&self.inner)
    }
    fn capability(&self) -> crate::opendal::Capability {
        crate::opendal::raw::Service::capability(&self.inner)
    }
    async fn create_dir(
        &self,
        ctx: &crate::opendal::OperationContext,
        path: &str,
        args: crate::opendal::raw::OpCreateDir,
    ) -> crate::opendal::Result<crate::opendal::raw::RpCreateDir> {
        crate::opendal::raw::Service::create_dir(&(), ctx, path, args).await
    }
    async fn stat(
        &self,
        ctx: &crate::opendal::OperationContext,
        path: &str,
        args: crate::opendal::raw::OpStat,
    ) -> crate::opendal::Result<crate::opendal::raw::RpStat> {
        self.events.record("STAT", path);
        if let Some(ms) = self.events.stat_advance.lock().unwrap().remove(path) {
            self.events.clock.advance(ms);
        }
        crate::opendal::raw::Service::stat(&self.inner, ctx, path, args).await
    }
    fn read(
        &self,
        ctx: &crate::opendal::OperationContext,
        path: &str,
        args: crate::opendal::raw::OpRead,
    ) -> crate::opendal::Result<Self::Reader> {
        crate::opendal::raw::Service::read(&(), ctx, path, args)
    }
    fn write(
        &self,
        ctx: &crate::opendal::OperationContext,
        path: &str,
        args: crate::opendal::raw::OpWrite,
    ) -> crate::opendal::Result<Self::Writer> {
        crate::opendal::raw::Service::write(&(), ctx, path, args)
    }
    fn delete(
        &self,
        ctx: &crate::opendal::OperationContext,
    ) -> crate::opendal::Result<Self::Deleter> {
        Ok(Box::new(DeleteRecorder {
            inner: crate::opendal::raw::Service::delete(&self.inner, ctx)?,
            events: self.events.clone(),
        }))
    }
    fn list(
        &self,
        ctx: &crate::opendal::OperationContext,
        path: &str,
        args: crate::opendal::raw::OpList,
    ) -> crate::opendal::Result<Self::Lister> {
        crate::opendal::raw::Service::list(&(), ctx, path, args)
    }
    fn copy(
        &self,
        ctx: &crate::opendal::OperationContext,
        from: &str,
        to: &str,
        args: crate::opendal::raw::OpCopy,
        opts: crate::opendal::raw::OpCopier,
    ) -> crate::opendal::Result<Self::Copier> {
        crate::opendal::raw::Service::copy(&(), ctx, from, to, args, opts)
    }
    async fn rename(
        &self,
        ctx: &crate::opendal::OperationContext,
        from: &str,
        to: &str,
        args: crate::opendal::raw::OpRename,
    ) -> crate::opendal::Result<crate::opendal::raw::RpRename> {
        crate::opendal::raw::Service::rename(&(), ctx, from, to, args).await
    }
    async fn presign(
        &self,
        ctx: &crate::opendal::OperationContext,
        path: &str,
        args: crate::opendal::raw::OpPresign,
    ) -> crate::opendal::Result<crate::opendal::raw::RpPresign> {
        crate::opendal::raw::Service::presign(&(), ctx, path, args).await
    }
}
struct DeleteRecorder {
    inner: oio::Deleter,
    events: Arc<Events>,
}
impl oio::Delete for DeleteRecorder {
    async fn delete(
        &mut self,
        path: &str,
        args: crate::opendal::raw::OpDelete,
    ) -> crate::opendal::Result<()> {
        // This fixture wraps Fs OneShotDeleter: delete, not handle creation
        // or close, is the real external operation in OpenDAL 0.58.1.
        self.events.record("DELETE", path);
        let consumes_time = !self.events.data_only_time || path.starts_with("data-");
        if !self.events.delete_delay.is_zero() {
            tokio::time::sleep(self.events.delete_delay).await;
        }
        let result = oio::Delete::delete(&mut self.inner, path, args).await;
        if consumes_time {
            self.events.clock.advance(self.events.delete_advance);
        }
        result
    }
    async fn close(&mut self) -> crate::opendal::Result<()> {
        oio::Delete::close(&mut self.inner).await
    }
}
struct TestStorage {
    io: FileIO,
    root: String,
    operator: crate::opendal::Operator,
    events: Arc<Events>,
}
impl ReclamationStorage for TestStorage {
    fn file_io(&self, _: &str, binding: IcebergReadBinding) -> FileIO {
        binding.request_context().unwrap().check_active().unwrap();
        // The grant is the production grant passed into actual FileIO.
        *self.events.actual_gc_deadline.lock().unwrap() =
            Some(binding.request_context().unwrap().deadline());
        self.io.clone()
    }
    fn delete_access(
        &self,
        location: &str,
        _: &IcebergReadBinding,
    ) -> Result<(crate::opendal::Operator, String), ConnectorError> {
        self.events.record("DELETE_ACCESS", location);
        self.events.delete_access_started.notify_one();
        let path = location
            .strip_prefix(&self.root)
            .ok_or_else(|| corrupt("Foreign oracle path"))?;
        Ok((self.operator.clone(), path.to_owned()))
    }
}

#[derive(Clone, Copy, Debug)]
enum FollowupMetadataChange {
    DisableGc,
    ReplaceUuid,
    UnreadableRoot,
}

struct Fixture {
    _dir: tempfile::TempDir,
    server: TestServer,
    owner: Arc<IcebergMetadataContext>,
    events: Arc<Events>,
    storage: TestStorage,
    retention: RetentionPlan,
    proof: CommitProof,
    stop: ConnectorStopOwner,
    original: ConnectorRequestContext,
    data: Vec<String>,
    retired_list: String,
    retired_manifest: String,
    followup_change: Arc<Mutex<Option<FollowupMetadataChange>>>,
}
impl Fixture {
    async fn new(
        fail_second: bool,
        read_ms: u64,
        stat_ms: u64,
        delete_ms: u64,
        response_ms: u64,
        real_delete_delay: Duration,
    ) -> Self {
        Self::new_grouped(
            fail_second,
            read_ms,
            stat_ms,
            delete_ms,
            response_ms,
            real_delete_delay,
            1,
            false,
        )
        .await
    }
    async fn new_grouped(
        fail_second: bool,
        read_ms: u64,
        stat_ms: u64,
        delete_ms: u64,
        response_ms: u64,
        real_delete_delay: Duration,
        groups: usize,
        data_only_time: bool,
    ) -> Self {
        assert!(groups == 1 || groups == 4);
        let dir = tempfile::tempdir().unwrap();
        let root = url::Url::from_directory_path(std::fs::canonicalize(dir.path()).unwrap())
            .unwrap()
            .to_string();
        let original_io = FileIO::new_with_fs();
        let clock = Arc::new(ManualClock::new());
        let events = Arc::new(Events {
            clock,
            log: Mutex::new(Vec::new()),
            read_advance: Default::default(),
            stat_advance: Default::default(),
            delete_advance: delete_ms,
            data_only_time,
            cancel_read: Mutex::new(None),
            delete_delay: real_delete_delay,
            actual_gc_deadline: Mutex::new(None),
            read_gate: Mutex::new(None),
            delete_access_started: tokio::sync::Notify::new(),
        });
        let mut base = super::tests::metadata();
        let mut json = serde_json::to_value(base).unwrap();
        json["location"] = serde_json::json!(root.trim_end_matches('/'));
        base = serde_json::from_value(json).unwrap();
        let manifest_path = format!("{root}retired-0.avro");
        let data = (0..4)
            .map(|i| format!("{root}data-{i}.parquet"))
            .collect::<Vec<_>>();
        let mut manifests = Vec::new();
        for group in 0..groups {
            let mut writer = ManifestWriterBuilder::new(
                original_io
                    .new_output(format!("{root}retired-{group}.avro"))
                    .unwrap(),
                Some(1),
                base.current_schema().clone(),
                base.default_partition_spec().as_ref().clone(),
            )
            .build_v2_data();
            for (i, path) in data.iter().enumerate().filter(|(i, _)| i % groups == group) {
                let _ = i;
                original_io
                    .new_output(path)
                    .unwrap()
                    .write(Bytes::from_static(b"x"))
                    .await
                    .unwrap();
                writer
                    .add_existing_file(
                        DataFileBuilder::default()
                            .content(DataContentType::Data)
                            .file_path(path.clone())
                            .file_format(DataFileFormat::Parquet)
                            .partition(Struct::empty())
                            .partition_spec_id(0)
                            .record_count(1)
                            .file_size_in_bytes(1)
                            .build()
                            .unwrap(),
                        1,
                        1,
                        Some(1),
                    )
                    .unwrap();
            }
            manifests.push(writer.write_manifest_file().await.unwrap());
        }
        let retired_list = format!("{root}retired-list.avro");
        let mut list = ManifestListWriter::v2(
            original_io
                .new_output(&retired_list)
                .unwrap()
                .writer()
                .await
                .unwrap(),
            1,
            None,
            1,
        );
        list.add_manifests(manifests.into_iter()).unwrap();
        list.close().await.unwrap();
        let live_list = format!("{root}live-list.avro");
        ManifestListWriter::v2(
            original_io
                .new_output(&live_list)
                .unwrap()
                .writer()
                .await
                .unwrap(),
            2,
            Some(1),
            2,
        )
        .close()
        .await
        .unwrap();
        let snap = |id, parent, path: String| {
            Snapshot::builder()
                .with_snapshot_id(id)
                .with_parent_snapshot_id(parent)
                .with_sequence_number(id)
                .with_timestamp_ms(id * 1000)
                .with_manifest_list(path)
                .with_schema_id(0)
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build()
        };
        let before = base
            .into_builder(None)
            .add_snapshot(snap(1, None, retired_list.clone()))
            .unwrap()
            .add_snapshot(snap(2, Some(1), live_list))
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference::new(2, SnapshotRetention::branch(None, None, None)),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let retention = RetentionPlan::compute(&before, 10000, Some(9000), Some(1)).unwrap();
        assert_eq!(
            retention
                .removed_snapshots
                .iter()
                .map(Snapshot::snapshot_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        let before_path = format!("{root}before.json");
        original_io
            .new_output(&before_path)
            .unwrap()
            .write(serde_json::to_vec(&before).unwrap().into())
            .await
            .unwrap();
        let actual = before
            .clone()
            .into_builder(Some(before_path.clone()))
            .remove_snapshots(&[1])
            .set_properties(HashMap::from([(
                MARKER.to_owned(),
                "exact-operation".to_owned(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let actual_path = format!("{root}actual.json");
        original_io
            .new_output(&actual_path)
            .unwrap()
            .write(serde_json::to_vec(&actual).unwrap().into())
            .await
            .unwrap();
        let response = serde_json::json!({"metadata-location":actual_path,"metadata":actual});
        let observed = events.clone();
        let followup_change = Arc::new(Mutex::new(None::<FollowupMetadataChange>));
        let changed = followup_change.clone();
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on_fn("GET", "/v1/namespaces/db/tables/t", move |_, index| {
                    observed.record("CATALOG_START", "db.t");
                    observed.clock.advance(response_ms);
                    if fail_second && index > 0 {
                        Reply::status(503)
                    } else {
                        let mut reply = response.clone();
                        if index > 0 {
                            match *changed.lock().unwrap() {
                                Some(FollowupMetadataChange::DisableGc) => {
                                    reply["metadata"]["properties"]["gc.enabled"] = serde_json::json!("false");
                                }
                                Some(FollowupMetadataChange::ReplaceUuid) => {
                                    // A deterministic foreign incarnation, never computed
                                    // from the provider's expected UUID or protection set.
                                    reply["metadata"]["table-uuid"] = serde_json::json!("00000000-0000-4000-8000-000000000003");
                                }
                                Some(FollowupMetadataChange::UnreadableRoot) => {
                                    let path = format!("{}/missing-new-root.avro",reply["metadata"]["location"].as_str().unwrap());
                                    reply["metadata"]["last-sequence-number"] = serde_json::json!(3);
                                    reply["metadata"]["snapshots"].as_array_mut().unwrap().push(serde_json::json!({
                                        "snapshot-id":3,"parent-snapshot-id":2,"sequence-number":3,"timestamp-ms":3000,
                                        "manifest-list":path,"schema-id":0,"summary":{"operation":"append"}
                                    }));
                                }
                                None => {}
                            }
                        }
                        Reply::json(200, &reply)
                    }
                }),
        );
        let handle = tokio::runtime::Handle::current();
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle.clone())),
        );
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "gc",
            &[
                ("iceberg.catalog.type".into(), "rest".into()),
                ("iceberg.catalog.uri".into(), server.url()),
                ("iceberg.catalog.warehouse".into(), root.clone()),
                (
                    crate::catalog::publication::RECLAMATION_FRESHNESS.into(),
                    "10".into(),
                ),
                (
                    crate::catalog::publication::RECLAMATION_TIMEOUT.into(),
                    "1000".into(),
                ),
                (
                    crate::catalog::publication::EVIDENCE_TIMEOUT.into(),
                    "20".into(),
                ),
            ],
        )
        .unwrap();
        let owner = Arc::new(
            IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                crate::resources::IcebergMetadataResources::new(binding, handle),
            )
            .unwrap(),
        );
        let io = FileIOBuilder::new(Arc::new(ReadFactory(ReadStorage {
            original: original_io,
            events: events.clone(),
        })))
        .build();
        let plain = crate::opendal::Operator::new(
            crate::opendal::services::Fs::default().root(dir.path().to_str().unwrap()),
        )
        .unwrap();
        let (context, inner) = plain.into_parts();
        let service: crate::opendal::raw::Servicer = Arc::new(DeleteAccess {
            inner,
            events: events.clone(),
        });
        let operator = crate::opendal::Operator::from_parts(context, service);
        events
            .read_advance
            .lock()
            .unwrap()
            .insert(manifest_path.clone(), read_ms);
        events
            .stat_advance
            .lock()
            .unwrap()
            .insert("data-0.parquet".into(), stat_ms);
        let mut proof = CommitProof::applied(None).with_table_uuid(actual.uuid().to_string());
        proof.metadata_location = Some(Arc::from(actual_path));
        proof.metadata_predecessor = MetadataPredecessorObservation::Candidate {
            location: before_path,
            timestamp_ms: before.last_updated_ms(),
        };
        let stop = ConnectorStopOwner::new();
        let original = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(10),
            stop.view(),
            65536,
            262144,
        )
        .unwrap();
        Self {
            _dir: dir,
            server,
            owner,
            events: events.clone(),
            storage: TestStorage {
                io,
                root,
                operator,
                events: events.clone(),
            },
            retention,
            proof,
            stop,
            original,
            data,
            retired_list,
            retired_manifest: manifest_path,
            followup_change,
        }
    }
    async fn run(&self) -> (Result<(), ConnectorError>, ScanCache, ReclamationReport) {
        let mut cache = ScanCache::default();
        let mut report = ReclamationReport::default();
        let result = reclaim_round_using(
            &ReclamationPorts {
                catalog: self.owner.novarocks_catalog().as_ref(),
                planning_binding: self.owner.resources().planning_binding(),
                clock: self.events.clock.as_ref(),
                storage: &self.storage,
            },
            CatalogTableName::new("db", "t"),
            &self.retention,
            "exact-operation",
            &self.proof,
            &self.original,
            &mut cache,
            &mut report,
        )
        .await;
        (result, cache, report)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_round_revalidates_after_scan_and_stat_wait_and_each_delete_is_within_t() {
    let f = Fixture::new(false, 11, 11, 4, 0, Duration::ZERO).await;
    let (result, cache, report) = f.run().await;
    result.unwrap();
    assert_eq!(report.deleted, 6);
    assert_eq!(report.pending, 0);
    let log = f.events.log.lock().unwrap().clone();
    let mut request = 0;
    let mut deletes = Vec::new();
    for e in log.iter() {
        match e.kind {
            "CATALOG_START" => request = e.ms,
            "DELETE" => {
                assert!(e.ms - request <= 10, "{e:?}, request={request}");
                deletes.push(e.ms)
            }
            _ => {}
        }
    }
    // Hand timeline: retired manifest scan takes 11; first stat adds 11;
    // dispatch starts 22,26,30; revalidate at34; then34,38,42.
    assert_eq!(deletes, vec![22, 26, 30, 34, 38, 42]);
    assert_eq!(
        log.iter()
            .filter(|e| e.kind == "CATALOG_START")
            .map(|e| e.ms)
            .collect::<Vec<_>>(),
        vec![0, 11, 22, 34]
    );
    assert_eq!(cache.manifest_reads, 1);
    assert_eq!(cache.list_reads, 2);
    assert_eq!(f.events.count("READ_HEAD"), 0);
    for p in &f.data {
        assert!(!f.storage.io.new_input(p).unwrap().exists().await.unwrap());
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_actual_refresh_stops_before_delete_without_renewing_eligibility() {
    let f = Fixture::new(true, 11, 0, 0, 0, Duration::ZERO).await;
    let (result, cache, report) = f.run().await;
    assert!(result.is_err());
    assert_eq!(cache.catalog_reads, 2);
    assert_eq!(report.delete_requests, 0);
    assert_eq!(f.events.count("DELETE"), 0);
    assert!(
        f.storage
            .io
            .new_input(&f.data[0])
            .unwrap()
            .exists()
            .await
            .unwrap()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_actual_catalog_response_is_explicit_freshness_exhausted_before_scan_or_delete() {
    let f = Fixture::new(false, 0, 0, 0, 11, Duration::ZERO).await;
    let (result, cache, report) = f.run().await;
    let error = result.unwrap_err();
    assert!(error.to_string().contains("freshness_exhausted"));
    assert_eq!(cache.catalog_reads, 1);
    assert_eq!(cache.list_reads, 0);
    assert_eq!(report.delete_requests, 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_work_completes_without_freshness_polling() {
    let mut f = Fixture::new(false, 0, 0, 0, 0, Duration::ZERO).await;
    f.retention.removed_snapshots.clear();
    let (result, cache, report) = f.run().await;
    result.unwrap();
    assert_eq!(cache.catalog_reads, 1);
    assert_eq!(report.delete_requests, 0);
    assert_eq!(f.server.count("GET", "/v1/namespaces/db/tables/t"), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_partial_real_manifest_read_registers_no_root_coverage_or_delete() {
    let f = Fixture::new(false, 0, 0, 0, 0, Duration::ZERO).await;
    *f.events.cancel_read.lock().unwrap() = Some((f.retired_manifest.clone(), f.stop.clone()));
    let (result, cache, report) = f.run().await;
    assert_eq!(result.unwrap_err().kind(), ConnectorErrorKind::Cancelled);
    assert!(!cache.roots.contains_key(&(1, f.retired_list.clone())));
    assert!(!cache.manifests.contains_key(&f.retired_manifest));
    assert_eq!(report.delete_requests, 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_round_uses_full_independent_gc_grant_after_evidence_is_nearly_spent() {
    let f = Fixture::new(false, 0, 0, 0, 0, Duration::from_millis(5)).await;
    let evidence = f
        .owner
        .novarocks_catalog()
        .evidence_policy()
        .begin_evidence(&f.original)
        .unwrap();
    tokio::time::sleep_until((evidence.deadline() - Duration::from_millis(1)).into()).await;
    let entered = Instant::now();
    let (result, _, report) = f.run().await;
    result.unwrap();
    assert!(
        f.events.actual_gc_deadline.lock().unwrap().unwrap()
            >= entered + Duration::from_millis(900)
    );
    assert!(evidence.expired());
    assert_eq!(report.deleted, 6);
    assert_eq!(report.pending, 0);
}

#[tokio::test]
async fn actual_nonempty_multiblob_puffin_footer_protects_the_whole_physical_object() {
    let original = FileIO::new_with_memory();
    let path = "memory://gc/two-dv.puffin";
    let live = "memory://gc/live.parquet";
    let dead = "memory://gc/retired.parquet";
    // Handwritten framing and hand-expected physical associations. The bitmap
    // encoder only supplies valid blob content; it computes no protection set.
    let mut bitmap = crate::commit::puffin_dv::DeletionVector::new();
    bitmap.insert(0).unwrap();
    let payload = bitmap.to_iceberg_payload().unwrap();
    let footer = serde_json::json!({"blobs":[
        {"type":"deletion-vector-v1","fields":[],"snapshot-id":1,"sequence-number":1,"offset":4,"length":payload.len(),"properties":{"referenced-data-file":live}},
        {"type":"deletion-vector-v1","fields":[],"snapshot-id":1,"sequence-number":1,"offset":4+payload.len(),"length":payload.len(),"properties":{"referenced-data-file":dead}}
    ]});
    let json = serde_json::to_vec(&footer).unwrap();
    let mut bytes = b"PFA1".to_vec();
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(b"PFA1");
    bytes.extend_from_slice(&json);
    bytes.extend_from_slice(&(json.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(b"PFA1");
    original
        .new_output(path)
        .unwrap()
        .write(bytes.clone().into())
        .await
        .unwrap();
    let events = Arc::new(Events {
        clock: Arc::new(ManualClock::new()),
        log: Default::default(),
        read_advance: Default::default(),
        stat_advance: Default::default(),
        delete_advance: 0,
        data_only_time: false,
        cancel_read: Default::default(),
        delete_delay: Duration::ZERO,
        actual_gc_deadline: Default::default(),
        read_gate: Mutex::new(None),
        delete_access_started: tokio::sync::Notify::new(),
    });
    let io = FileIOBuilder::new(Arc::new(ReadFactory(ReadStorage {
        original,
        events: events.clone(),
    })))
    .build();
    let mut cache = ScanCache::default();
    let actual = cache
        .puffin_associations(
            &BTreeSet::from([path.to_owned()]),
            &BTreeMap::from([(path.to_owned(), bytes.len() as u64)]),
            &io,
        )
        .await
        .unwrap();
    assert_eq!(
        actual,
        BTreeMap::from([(
            path.to_owned(),
            BTreeSet::from([live.to_owned(), dead.to_owned()])
        )])
    );
    let candidates = Reachability {
        paths: BTreeSet::from([path.to_owned(), dead.to_owned()]),
        dv: actual,
        ..Default::default()
    };
    let mut protection = Reachability {
        data: BTreeSet::from([live.to_owned()]),
        ..Default::default()
    };
    protect_half_references(&candidates, &mut protection);
    assert_eq!(protection.paths, BTreeSet::from([path.to_owned()]));
    assert_eq!(
        candidates
            .paths
            .difference(&protection.paths)
            .cloned()
            .collect::<Vec<_>>(),
        vec![dead.to_owned()]
    );
    assert_eq!(events.count("READ_HEAD"), 0);
    assert_eq!(events.count("RANGE_OPEN"), 1);
    assert!(io.new_input(path).unwrap().exists().await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_virtual_timeline_different_manifest_group_sizes_does_not_multiply_catalog_revalidation()
 {
    // There is no production batch-size setting: the current owner is serial.
    // Exercise the real grouping boundary available today: four data objects
    // in one manifest vs four manifests. Structural deletes take zero virtual
    // time, data deletes four ms each, and the first scan/stat each consume11.
    // Expected catalog timeline is hand-authored and independent of grouping.
    for groups in [1, 4] {
        let f = Fixture::new_grouped(false, 11, 11, 4, 0, Duration::ZERO, groups, true).await;
        let (result, cache, report) = f.run().await;
        result.unwrap();
        let events = f.events.log.lock().unwrap().clone();
        let mut request = 0;
        for e in events.iter() {
            match e.kind {
                "CATALOG_START" => request = e.ms,
                "DELETE" => assert!(e.ms - request <= 10, "{e:?}"),
                _ => {}
            }
        }
        assert_eq!(
            events
                .iter()
                .filter(|e| e.kind == "CATALOG_START")
                .map(|e| e.ms)
                .collect::<Vec<_>>(),
            vec![0, 11, 22, 34]
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| e.kind == "DELETE" && e.path.starts_with("data-"))
                .map(|e| e.ms)
                .collect::<Vec<_>>(),
            vec![22, 26, 30, 34]
        );
        assert_eq!(cache.catalog_reads, 4);
        assert_eq!(cache.list_reads, 2);
        assert_eq!(cache.manifest_reads, groups);
        assert_eq!(report.deleted, 5 + groups);
        assert_eq!(f.events.count("READ_HEAD"), 0);
    }
}

// Use the existing owner admission API, never access its private semaphore.
// Every holder is actually admitted before the caller continues; release is
// remembered by the gate and every JoinHandle is explicitly awaited below.
async fn occupy_owner_positions(
    f: &Fixture,
) -> (
    Arc<tokio::sync::Semaphore>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut holders = Vec::new();
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let admission = f.owner.novarocks_catalog().listing_admission();
    for _ in 0..crate::catalog::listing_admission::LISTING_CONCURRENCY {
        let admission = admission.clone();
        let context = f.original.clone();
        let gate = gate.clone();
        let entered = entered_tx.clone();
        holders.push(tokio::spawn(async move {
            admission
                .run_wait_for_exit(&context, async move {
                    entered.send(()).unwrap();
                    let _release = gate.acquire().await.unwrap();
                    Ok(())
                })
                .await
                .unwrap();
        }));
    }
    drop(entered_tx);
    for _ in 0..crate::catalog::listing_admission::LISTING_CONCURRENCY {
        entered_rx.recv().await.unwrap();
    }
    assert_eq!(admission.available_positions(), 0);
    (gate, holders)
}
async fn release_owner_positions(
    gate: Arc<tokio::sync::Semaphore>,
    holders: Vec<tokio::task::JoinHandle<()>>,
) {
    gate.add_permits(holders.len());
    for holder in holders {
        holder.await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_admission_queue_consumes_t_and_requires_refresh_before_any_delete() {
    let f = Arc::new(Fixture::new(false, 0, 0, 0, 0, Duration::ZERO).await);
    let scan_entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *f.events.read_gate.lock().unwrap() = Some((
        f.retired_manifest.clone(),
        scan_entered.clone(),
        resume.clone(),
    ));
    let task = {
        let f = f.clone();
        tokio::spawn(async move { f.run().await })
    };
    tokio::time::timeout(Duration::from_secs(1), scan_entered.notified())
        .await
        .unwrap();
    let (gate, holders) = occupy_owner_positions(&f).await;
    resume.notify_one();
    tokio::time::timeout(
        Duration::from_secs(1),
        f.events.delete_access_started.notified(),
    )
    .await
    .unwrap();
    assert_eq!(f.events.count("STAT"), 0);
    assert_eq!(f.events.count("DELETE"), 0);
    // The request was ready at0, but the real admission semaphore is full.
    f.events.clock.advance(11);
    release_owner_positions(gate, holders).await;
    let (result, cache, report) = task.await.unwrap();
    result.unwrap();
    assert_eq!(cache.catalog_reads, 2);
    assert_eq!(report.deleted, 6);
    let events = f.events.log.lock().unwrap().clone();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == "CATALOG_START")
            .map(|e| e.ms)
            .collect::<Vec<_>>(),
        vec![0, 11]
    );
    assert!(
        events
            .iter()
            .filter(|e| e.kind == "DELETE")
            .all(|e| e.ms == 11)
    );
    assert_eq!(
        f.owner
            .novarocks_catalog()
            .listing_admission()
            .available_positions(),
        crate::catalog::listing_admission::LISTING_CONCURRENCY
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_gc_grant_preserves_freshness_exhausted_context_after_queue_wait() {
    let f = Arc::new(Fixture::new(false, 0, 0, 0, 0, Duration::ZERO).await);
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *f.events.read_gate.lock().unwrap() =
        Some((f.retired_manifest.clone(), entered.clone(), resume.clone()));
    let mut task = {
        let f = f.clone();
        tokio::spawn(async move { f.run().await })
    };
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    let (gate, holders) = occupy_owner_positions(&f).await;
    resume.notify_one();
    tokio::time::timeout(
        Duration::from_secs(1),
        f.events.delete_access_started.notified(),
    )
    .await
    .unwrap();
    f.events.clock.advance(11);
    // Real finite GC policy1000ms expires; virtual freshness is not a grant.
    let output = tokio::time::timeout(Duration::from_secs(2), &mut task).await;
    // Always release/join the holders, even before asserting task outcome.
    release_owner_positions(gate, holders).await;
    let completed = match output {
        Ok(completed) => completed,
        Err(_) => {
            let completed = task.await;
            panic!(
                "GC missed its actual-exit readiness bound after holders were joined: task_ok={}",
                completed.is_ok()
            );
        }
    };
    let (result, _, report) = completed.unwrap();
    let error = result.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
    assert!(error.message().contains("freshness_exhausted"), "{error}");
    assert_eq!(report.delete_requests, 0);
    assert_eq!(f.events.count("DELETE"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn applied_round_rechecks_gc_and_uuid_on_actual_second_get_before_any_delete() {
    for (change, kind, diagnostic) in [
        (
            FollowupMetadataChange::DisableGc,
            ConnectorErrorKind::Unsupported,
            "gc.enabled is false",
        ),
        (
            FollowupMetadataChange::ReplaceUuid,
            ConnectorErrorKind::CorruptData,
            "table UUID changed",
        ),
    ] {
        let f = Fixture::new(false, 11, 0, 0, 0, Duration::ZERO).await;
        *f.followup_change.lock().unwrap() = Some(change);
        let (result, cache, report) = f.run().await;
        let error = result.expect_err("A changed current authorization cannot authorize DELETE");
        assert_eq!(error.kind(), kind, "{change:?}: {error}");
        assert!(error.message().contains(diagnostic), "{change:?}: {error}");
        assert_eq!(
            f.proof.effect,
            novarocks_spi::connector::ExternalMutationEffect::Applied
        );
        assert!(report.audit_complete);
        assert_eq!(cache.catalog_reads, 2);
        assert_eq!(f.server.count("GET", "/v1/namespaces/db/tables/t"), 2);
        assert_eq!(
            f.events
                .log
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.kind == "CATALOG_START")
                .map(|e| e.ms)
                .collect::<Vec<_>>(),
            vec![0, 11]
        );
        assert_eq!(report.delete_requests, 0);
        assert_eq!(report.deleted, 0);
        assert_eq!(report.pending, 6);
        assert_eq!(f.events.count("DELETE"), 0);
        assert_eq!(f.events.count("STAT"), 0);
        for candidate in f.data.iter().chain([&f.retired_list, &f.retired_manifest]) {
            assert!(
                f.storage
                    .io
                    .new_input(candidate)
                    .unwrap()
                    .exists()
                    .await
                    .unwrap(),
                "Candidate was deleted after {change:?}: {candidate}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn applied_round_stops_when_actual_second_get_adds_an_unreadable_root() {
    let f = Fixture::new(false, 11, 0, 0, 0, Duration::ZERO).await;
    *f.followup_change.lock().unwrap() = Some(FollowupMetadataChange::UnreadableRoot);
    let (result, cache, report) = f.run().await;
    let error = result.expect_err("Incomplete new root cannot renew deletion qualification");
    assert!(error.message().contains("missing-new-root.avro"), "{error}");
    assert_eq!(cache.catalog_reads, 2);
    assert_eq!(
        f.events
            .log
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.kind == "CATALOG_START")
            .map(|e| e.ms)
            .collect::<Vec<_>>(),
        vec![0, 11]
    );
    assert!(cache.roots.keys().all(|(id, _)| *id != 3));
    assert_eq!(f.events.count("DELETE"), 0);
    assert_eq!(report.delete_requests, 0);
    assert!(
        f.storage
            .io
            .new_input(&f.data[0])
            .unwrap()
            .exists()
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_actual_fresh_qualification_does_not_replace_held_view_or_request_time() {
    for change in [
        FollowupMetadataChange::DisableGc,
        FollowupMetadataChange::ReplaceUuid,
        FollowupMetadataChange::UnreadableRoot,
    ] {
        let f = Fixture::new(false, 0, 0, 0, 0, Duration::ZERO).await;
        let binding = f
            .owner
            .resources()
            .planning_binding()
            .for_request(f.original.clone());
        let mut view = None;
        let mut cache = ScanCache::default();
        let mut protected = Reachability::default();
        let uuid = uuid::Uuid::parse_str(f.proof.table_uuid.as_deref().unwrap()).unwrap();
        refresh(
            f.owner.novarocks_catalog().as_ref(),
            f.events.clock.as_ref(),
            &CatalogTableName::new("db", "t"),
            &binding,
            &mut view,
            uuid,
            &mut cache,
            &f.storage.io,
            &mut protected,
        )
        .await
        .unwrap();
        let previous = view.as_ref().unwrap();
        let previous_metadata = serde_json::to_value(&previous.base.metadata).unwrap();
        let previous_location = previous.base.metadata_location.clone();
        let previous_time = previous.requested;
        let previous_protected = protected.paths.clone();
        *f.followup_change.lock().unwrap() = Some(change);
        f.events.clock.advance(11);
        let result = refresh(
            f.owner.novarocks_catalog().as_ref(),
            f.events.clock.as_ref(),
            &CatalogTableName::new("db", "t"),
            &binding,
            &mut view,
            uuid,
            &mut cache,
            &f.storage.io,
            &mut protected,
        )
        .await;
        assert!(result.is_err(), "{change:?}");
        let retained = view.as_ref().unwrap();
        assert_eq!(retained.requested, previous_time, "{change:?}");
        assert_eq!(
            retained.base.metadata_location, previous_location,
            "{change:?}"
        );
        assert_eq!(
            serde_json::to_value(&retained.base.metadata).unwrap(),
            previous_metadata,
            "{change:?}"
        );
        assert_eq!(protected.paths, previous_protected, "{change:?}");
        assert_eq!(
            f.events
                .log
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.kind == "CATALOG_START")
                .map(|e| e.ms)
                .collect::<Vec<_>>(),
            vec![0, 11]
        );
        assert_eq!(f.events.count("DELETE"), 0);
    }
}
