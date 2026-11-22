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
use crate::commit::operation::OperationLimits;
use crate::commit::statistics_carry::CarryForwardStatisticsPreparer;
use crate::iceberg::puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter};
use crate::iceberg::spec::{BlobMetadata, Operation};
use novarocks_spi::connector::{ConnectorPayloadByteBudget, ConnectorPayloadRetentionGuard};

struct StagedReplace;

#[async_trait]
impl Preparer for StagedReplace {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> Result<PreparedChange> {
        crate::commit::overwrite::prepare_snapshot_change(
            view,
            intent,
            view.stage_snapshot_id()?,
            Operation::Replace,
            Vec::new(),
            HashMap::new(),
            false,
        )
        .await
    }
}

#[derive(Default)]
struct ByteAuthor {
    live: AtomicUsize,
    peak: AtomicUsize,
    limit: AtomicUsize,
}

struct FixtureAuthor(Arc<ByteAuthor>);
struct Backing(Arc<ByteAuthor>, usize);

impl Drop for Backing {
    fn drop(&mut self) {
        self.0.live.fetch_sub(self.1, Ordering::SeqCst);
    }
}

impl ConnectorPayloadByteBudget for FixtureAuthor {
    fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> std::result::Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        let next = self
            .0
            .live
            .load(Ordering::SeqCst)
            .checked_add(bytes)
            .unwrap();
        if next > self.0.limit.load(Ordering::SeqCst) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "original test Root byte budget exhausted",
            ));
        }
        self.0.live.store(next, Ordering::SeqCst);
        self.0.peak.fetch_max(next, Ordering::SeqCst);
        Ok(ConnectorPayloadRetentionGuard::new(Backing(
            Arc::clone(&self.0),
            bytes,
        )))
    }
}

fn fixture() -> Fixture {
    Fixture::new_with_limits(OperationLimits {
        io_requests: 1,
        ..OperationLimits::default()
    })
}

fn author(limit: usize) -> (Arc<ByteAuthor>, ConnectorPayloadRetentionGuard) {
    let root = Arc::new(ByteAuthor::default());
    root.limit.store(limit, Ordering::SeqCst);
    (
        root.clone(),
        ConnectorPayloadRetentionGuard::from_byte_budget(Arc::new(FixtureAuthor(root))),
    )
}

fn file_path(uri: &str) -> &std::path::Path {
    std::path::Path::new(uri.strip_prefix("file://").unwrap())
}

async fn source_file(fixture: &Fixture, metadata: &TableMetadata, name: &str) -> StatisticsFile {
    let id = metadata.current_snapshot().unwrap().snapshot_id();
    let sequence_number = metadata.current_snapshot().unwrap().sequence_number();
    let path = format!(
        "file://{}/{name}.puffin",
        fixture.directory.path().display()
    );
    let io = crate::iceberg::io::FileIO::new_with_fs();
    let output = io.new_output(&path).unwrap();
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
        .await
        .unwrap();
    let blob_type = "test.rewrite.statistics".to_owned();
    let properties = HashMap::from([("ndv".to_owned(), "3".to_owned())]);
    let blob = Blob::builder()
        .r#type(blob_type.clone())
        .fields(vec![1])
        .snapshot_id(id)
        .sequence_number(sequence_number)
        .data(vec![1, 2, 3])
        .properties(properties.clone())
        .build();
    writer
        .add(blob, CompressionCodec::zstd_default())
        .await
        .unwrap();
    let closed = writer.close().await.unwrap();
    let bytes = std::fs::read(file_path(&path)).unwrap();
    let encoded = &bytes[bytes.len() - 12..bytes.len() - 8];
    let footer = 16 + i64::from(u32::from_le_bytes(encoded.try_into().unwrap()));
    StatisticsFile {
        snapshot_id: id,
        statistics_path: path,
        file_size_in_bytes: closed.size.try_into().unwrap(),
        file_footer_size_in_bytes: footer,
        key_metadata: None,
        blob_metadata: vec![BlobMetadata {
            r#type: blob_type,
            snapshot_id: id,
            sequence_number,
            fields: vec![1],
            properties,
        }],
    }
}

fn register(base: TableMetadata, statistics: StatisticsFile) -> TableMetadata {
    TableUpdate::SetStatistics { statistics }
        .apply(base.into_builder(None))
        .unwrap()
        .build()
        .unwrap()
        .metadata
}

async fn run_real(
    fixture: &Fixture,
    base: &TableMetadata,
    publisher: &ControlledPublisher<'_>,
    author: &ConnectorPayloadRetentionGuard,
) -> (Report, Vec<crate::fs_io::TestStorageRequest>) {
    let original = &fixture.operation;
    let intent = fixture.intent(base, "main", Vec::new());
    let preparer = CarryForwardStatisticsPreparer::new(original, 0, author);
    let trace = Arc::new(Mutex::new(Vec::new()));
    let report = tokio::time::timeout(
        Duration::from_secs(10),
        crate::fs_io::TEST_STORAGE_REQUESTS.scope(
            trace.clone(),
            run(
                original,
                &intent,
                &PreparedChange::default(),
                &[&StagedReplace, &preparer],
                publisher,
                policy(),
            ),
        ),
    )
    .await
    .expect("the physical copy must make progress with one operation I/O slot");
    (
        report,
        Arc::try_unwrap(trace).unwrap().into_inner().unwrap(),
    )
}

#[tokio::test]
async fn statistics_carry_one_slot_retains_physical_copy_and_refreshes_registration() {
    let fixture = fixture();
    let seed = Fixture::new();
    let original = append_state(&seed, fixture.metadata(FormatVersion::V2), 1).await;
    let source = source_file(&fixture, &original, "current").await;
    let mut first_registration = source.clone();
    first_registration.blob_metadata[0].properties.clear();
    let base = register(original, first_registration.clone());
    let competitor = Fixture::new();
    let advanced = append_state(&competitor, base.clone(), 2).await;
    let advanced = register(advanced, source.clone());
    let publisher = ControlledPublisher::new(
        &fixture,
        base.clone(),
        Dispatch::ConflictThen(Box::new(advanced)),
    );
    let (root, author) = author(usize::MAX);
    let preexisting = author.reserve_owned_bytes(127).unwrap();
    let (report, trace) = run_real(&fixture, &base, &publisher, &author).await;
    assert!(
        matches!(report.publication, PublicationOutcome::Committed(_)),
        "{report:?}"
    );

    let requests = publisher.requests.lock().unwrap();
    let [first, last] = requests.as_slice() else {
        panic!("expected exactly two rounds")
    };
    let old = requested_copy(first);
    let new = requested_copy(last);
    assert_eq!(old.statistics_path, new.statistics_path);
    assert_eq!(old.snapshot_id, new.snapshot_id);
    let sequence = |request: &serde_json::Value| {
        request["updates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|update| update["action"] == "add-snapshot")
            .unwrap()["snapshot"]["sequence-number"]
            .as_i64()
            .unwrap()
    };
    assert!(sequence(first) < sequence(last));
    assert_eq!(old.blob_metadata, first_registration.blob_metadata);
    assert_eq!(new.blob_metadata, source.blob_metadata);
    assert_eq!(
        std::fs::read(file_path(&new.statistics_path)).unwrap(),
        std::fs::read(file_path(&source.statistics_path)).unwrap()
    );
    assert_eq!(
        new.file_footer_size_in_bytes,
        source.file_footer_size_in_bytes
    );
    let io = crate::iceberg::io::FileIO::new_with_fs();
    let reader = PuffinReader::new(io.new_input(&new.statistics_path).unwrap())
        .await
        .unwrap();
    let metadata = reader.file_metadata().await.unwrap();
    let blob = &metadata.blobs()[0];
    assert_eq!(blob.snapshot_id(), source.snapshot_id);
    assert_eq!(
        blob.sequence_number(),
        source.blob_metadata[0].sequence_number
    );
    assert_eq!(reader.blob(blob).await.unwrap().data(), &[1, 2, 3]);

    let selected = |action, path: &str| {
        trace
            .iter()
            .enumerate()
            .filter(|(_, event)| event.operation == action && event.path == path)
            .map(|(i, _)| i)
            .collect::<Vec<_>>()
    };
    let reads = selected("statistics-read", &source.statistics_path);
    let writes = selected("write", &new.statistics_path);
    let commits = selected("dispatch", "db.t");
    assert_eq!((reads.len(), writes.len(), commits.len()), (1, 1, 2));
    assert!(reads[0] < writes[0] && writes[0] < commits[0]);
    let live = root.live.load(Ordering::SeqCst);
    assert!(live > 127);
    assert!(root.peak.load(Ordering::SeqCst) >= live + 2 * source.file_size_in_bytes as usize);
    drop(requests);
    drop(publisher);
    drop(fixture);
    assert_eq!(root.live.load(Ordering::SeqCst), 127);
    drop(preexisting);
    assert_eq!(root.live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn statistics_carry_releases_known_failure_and_retains_unknown() {
    for unknown in [false, true] {
        let fixture = fixture();
        let seed = Fixture::new();
        let original = append_state(&seed, fixture.metadata(FormatVersion::V2), 1).await;
        let source = source_file(&fixture, &original, "source").await;
        let base = register(original, source.clone());
        let publisher = ControlledPublisher::new(
            &fixture,
            base.clone(),
            if unknown {
                Dispatch::Unknown
            } else {
                Dispatch::AlwaysConflict
            },
        );
        let (_, author) = author(usize::MAX);
        let (report, _) = run_real(&fixture, &base, &publisher, &author).await;
        let requests = publisher.requests.lock().unwrap();
        let [request] = requests.as_slice() else {
            panic!("one dispatch")
        };
        let output = requested_copy(request);
        assert!(file_path(&source.statistics_path).is_file());
        assert_eq!(file_path(&output.statistics_path).is_file(), unknown);
        if unknown {
            assert!(matches!(
                report.publication,
                PublicationOutcome::Unknown { .. }
            ));
            assert_eq!(report.cleanup, IcebergCleanupReport::NotAttempted);
        } else {
            assert!(matches!(
                report.publication,
                PublicationOutcome::KnownUncommitted(_)
            ));
            assert!(matches!(
                report.cleanup,
                IcebergCleanupReport::Complete { .. }
            ));
        }
    }
}

#[tokio::test]
async fn statistics_carry_skips_missing_source_and_rejects_corrupt_or_unpaid_copy() {
    for (source_present, corrupt, limit) in [
        (false, false, 0),
        (true, true, usize::MAX),
        (true, false, 0),
    ] {
        let fixture = fixture();
        let seed = Fixture::new();
        let mut base = append_state(&seed, fixture.metadata(FormatVersion::V2), 1).await;
        if source_present {
            let mut source = source_file(&fixture, &base, "source").await;
            if corrupt {
                // Valid numeric framing shape, but it disagrees with the physical object.
                source.file_size_in_bytes += 1;
            }
            base = register(base, source);
        }
        let publisher = ControlledPublisher::new(&fixture, base.clone(), Dispatch::Commit);
        let admission_limit = if source_present && !corrupt {
            let source = base
                .statistics_for_snapshot(base.current_snapshot().unwrap().snapshot_id())
                .unwrap();
            let footprint = crate::stats_assembler::statistics_carry_backing(
                source,
                fixture.operation.statistics_artifact_path_bound().unwrap(),
            )
            .unwrap();
            // The long-lived backing fits. Refuse only the peak that must be
            // paid before the range I/O can consume the single slot.
            footprint.registration + footprint.reuse + 2 * source.file_size_in_bytes as usize - 1
        } else {
            limit
        };
        let (root, author) = author(admission_limit);
        let (report, trace) = run_real(&fixture, &base, &publisher, &author).await;
        assert!(!trace.iter().any(|event| event.path.ends_with(".puffin")));
        let requests = publisher.requests.lock().unwrap();
        if source_present {
            let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
                panic!("expected a pre-dispatch refusal");
            };
            assert_eq!(
                failure.kind(),
                if corrupt {
                    ConnectorMutationFailureKind::CorruptData
                } else {
                    ConnectorMutationFailureKind::ResourceExhausted
                }
            );
            assert!(requests.is_empty());
        } else {
            assert!(matches!(
                report.publication,
                PublicationOutcome::Committed(_)
            ));
            assert!(
                !requests[0]["updates"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|update| update["action"] == "set-statistics")
            );
        }
        assert_eq!(root.live.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn statistics_carry_reconfirms_changed_source_or_refuses_bad_footer() {
    for corrupt_footer in [false, true] {
        let fixture = fixture();
        let seed = Fixture::new();
        let original = append_state(&seed, fixture.metadata(FormatVersion::V2), 1).await;
        let source = source_file(&fixture, &original, "source").await;
        let base = register(original, source.clone());
        let competitor = Fixture::new();
        let advanced = append_state(&competitor, base.clone(), 2).await;
        let next = if corrupt_footer {
            let mut next = source.clone();
            next.file_footer_size_in_bytes -= 1;
            next
        } else {
            source_file(&fixture, &advanced, "new-source").await
        };
        let advanced = register(advanced, next.clone());
        let publisher = ControlledPublisher::new(
            &fixture,
            base.clone(),
            Dispatch::ConflictThen(Box::new(advanced)),
        );
        let (_, author) = author(usize::MAX);
        let (report, trace) = run_real(&fixture, &base, &publisher, &author).await;
        let requests = publisher.requests.lock().unwrap();
        let discarded = requested_copy(&requests[0]);
        assert!(file_path(&source.statistics_path).is_file());
        assert!(file_path(&next.statistics_path).is_file());
        assert!(!file_path(&discarded.statistics_path).exists());
        assert_eq!(
            trace
                .iter()
                .filter(|event| event.operation == "statistics-read")
                .count(),
            2
        );
        if corrupt_footer {
            let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
                panic!("expected invalid footer to prevent the second dispatch");
            };
            assert_eq!(failure.kind(), ConnectorMutationFailureKind::CorruptData);
            assert_eq!(requests.len(), 1);
        } else {
            assert!(matches!(
                report.publication,
                PublicationOutcome::Committed(_)
            ));
            assert_eq!(requests.len(), 2);
            let published = requested_copy(&requests[1]);
            assert_ne!(published.statistics_path, discarded.statistics_path);
            assert_eq!(
                std::fs::read(file_path(&published.statistics_path)).unwrap(),
                std::fs::read(file_path(&next.statistics_path)).unwrap()
            );
        }
    }
}

#[tokio::test]
async fn statistics_carry_keeps_the_original_lz4_footer_frame() {
    use base64::Engine;

    // Iceberg framing with a content-sized LZ4 frame of a JSON footer.
    // The raw path can carry compressed footers even if a reader does not
    // support their codec.
    let bytes = base64::engine::general_purpose::STANDARD
        .decode("UEZBMVBGQTEEIk0YbEBAAAAAAAAAANpAAACAeyJibG9icyI6W10sInByb3BlcnRpZXMiOnsib3JpZ2luIjoiY29tcHJlc3NlZC1mb290ZXItZml4dHVyZSJ9fQAAAAAMds5fWwAAAAEAAABQRkEx")
        .unwrap();
    let fixture = fixture();
    let seed = Fixture::new();
    let base = append_state(&seed, fixture.metadata(FormatVersion::V2), 1).await;
    let path = format!(
        "file://{}/compressed-footer.puffin",
        fixture.directory.path().display()
    );
    std::fs::write(file_path(&path), &bytes).unwrap();
    let id = base.current_snapshot().unwrap().snapshot_id();
    let base = register(
        base,
        StatisticsFile {
            snapshot_id: id,
            statistics_path: path,
            file_size_in_bytes: i64::try_from(bytes.len()).unwrap(),
            file_footer_size_in_bytes: i64::try_from(bytes.len() - 4).unwrap(),
            key_metadata: None,
            blob_metadata: Vec::new(),
        },
    );
    let publisher = ControlledPublisher::new(&fixture, base.clone(), Dispatch::Commit);
    let (_, author) = author(usize::MAX);
    let (report, _) = run_real(&fixture, &base, &publisher, &author).await;
    assert!(
        matches!(report.publication, PublicationOutcome::Committed(_)),
        "{report:?}"
    );
    let output = requested_copy(&publisher.requests.lock().unwrap()[0]);
    assert_eq!(
        std::fs::read(file_path(&output.statistics_path)).unwrap(),
        bytes
    );
}
