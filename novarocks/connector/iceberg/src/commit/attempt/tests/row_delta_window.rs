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

//! Checks retry and publication decisions through the original operation loop.

use super::*;
use crate::commit::conflict::{CandidateFile, MayMatch, RowDeltaValidationFacts};
use crate::commit::dependency::RowDeltaWindowCheck;
use crate::commit::fast_append::CompactFastAppendPreparer;
use crate::commit::model::IsolationLevel;
use crate::commit::new_manifests::tests::install_appends;
use novarocks_failure_contract::FailureProjection;
use novarocks_failure_contract::connector_reason::{
    COMMIT_TRANSACTION_CONFLICT, ICEBERG_COMMIT_STATE_UNKNOWN, ICEBERG_COMMIT_VALIDATION_CONFLICT,
};

struct AllFiles;

impl std::fmt::Display for AllFiles {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("true")
    }
}

impl MayMatch for AllFiles {
    fn may_match(&self, _file: &CandidateFile<'_>) -> std::result::Result<bool, ConnectorError> {
        Ok(true)
    }
}

fn row_delta_intent(
    fixture: &Fixture,
    metadata: &TableMetadata,
    isolation: IsolationLevel,
) -> OperationIntent {
    let ordinary = fixture.intent(metadata, "main", Vec::new());
    OperationIntent::new(OperationIntentParts {
        target: ordinary.target().clone(),
        target_ref: ordinary.target_ref().into(),
        start: ordinary.start(),
        changes: FileChanges::default(),
        dependencies: Vec::new(),
        protocol: crate::commit::model::CommitProtocol::RowDelta {
            window: RowDeltaWindowCheck::new(
                isolation,
                Arc::new(AllFiles),
                Arc::new(RowDeltaValidationFacts::default()),
                true,
                false,
            ),
            writer: crate::commit::model::WriterSchemaSpec {
                schema_id: metadata.current_schema_id(),
                default_spec_id: metadata.default_partition_spec_id(),
            },
        },
        shape: ordinary.shape(),
        summary: Default::default(),
        token: fixture.operation.token(),
    })
    .unwrap()
    .with_compact_stage(0)
    .unwrap()
}

#[tokio::test]
async fn serializable_window_checks_the_new_base_before_a_second_dispatch() {
    let seed = Fixture::new();
    let base = append_state(&seed, seed.metadata(FormatVersion::V3), 2).await;
    let winner = Fixture::new();
    let advanced = append_state(&winner, base.clone(), 4).await;
    let fixture = Fixture::new();
    install_appends(&fixture, &base, 1);
    let intent = row_delta_intent(&fixture, &base, IsolationLevel::Serializable);
    let publisher =
        ControlledPublisher::new(&fixture, base, Dispatch::ConflictThen(Box::new(advanced)));
    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&CompactFastAppendPreparer(fixture.operation.clone())],
        &publisher,
        policy(),
    )
    .await;
    let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
        panic!("expected concurrent matching data to refuse preparation: {report:?}");
    };
    assert_eq!(
        failure.failure_fact().reason(),
        ICEBERG_COMMIT_VALIDATION_CONFLICT
    );
    assert!(failure.message().starts_with("Found conflicting files"));
    assert_eq!(publisher.requests.lock().unwrap().len(), 1);
    assert_eq!(
        *publisher.load_modes.lock().unwrap(),
        [crate::catalog::SnapshotLoading::All; 2]
    );
    assert!(
        matches!(report.cleanup, IcebergCleanupReport::Complete { .. }),
        "{:?}",
        report.cleanup
    );
}

#[tokio::test]
async fn snapshot_retry_keeps_its_start_and_reuses_one_new_manifest() {
    let seed = Fixture::new();
    let base = append_state(&seed, seed.metadata(FormatVersion::V3), 2).await;
    let winner = Fixture::new();
    let advanced = append_state(&winner, base.clone(), 4).await;
    let second = append_state(&winner, advanced.clone(), 6).await;
    let advanced_id = second.current_snapshot_id();
    let fixture = Fixture::new();
    install_appends(&fixture, &base, 1);
    let intent = row_delta_intent(&fixture, &base, IsolationLevel::Snapshot);
    let publisher = ControlledPublisher::new(
        &fixture,
        base.clone(),
        Dispatch::ConflictSequence(vec![advanced, second]),
    );
    let trace = Arc::new(Mutex::new(Vec::new()));
    let report = crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(
            trace.clone(),
            run(
                &fixture.operation,
                &intent,
                &PreparedChange::default(),
                &[&CompactFastAppendPreparer(fixture.operation.clone())],
                &publisher,
                policy(),
            ),
        )
        .await;
    assert!(
        matches!(report.publication, PublicationOutcome::Committed(_)),
        "{report:?}"
    );
    assert_eq!(
        intent.start().map(|start| start.snapshot_id),
        base.current_snapshot_id()
    );
    assert_eq!(
        publisher
            .metadata
            .lock()
            .unwrap()
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id(),
        advanced_id
    );
    assert_eq!(publisher.requests.lock().unwrap().len(), 3);
    let supports = publisher.artifact_requests.lock().unwrap();
    let manifest = supports[0]
        .iter()
        .find(|object| {
            object.path().ends_with(".avro") && supports[1..].iter().all(|set| set.contains(object))
        })
        .expect("same stage retained its new manifest");
    assert_eq!(
        trace
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.path == manifest.path())
            .count(),
        1
    );
}

#[tokio::test]
async fn known_catalog_exhaustion_and_unknown_outcomes_keep_distinct_facts() {
    for expect_unknown in [false, true] {
        let seed = Fixture::new();
        let source = append_state(&seed, seed.metadata(FormatVersion::V3), 2).await;
        let fixture = Fixture::new();
        let intent = row_delta_intent(&fixture, &source, IsolationLevel::Snapshot);
        install_appends(&fixture, &source, 1);
        let retained = fixture.directory.path().join("data/compact-0000.parquet");
        std::fs::create_dir_all(retained.parent().unwrap()).unwrap();
        std::fs::write(&retained, b"received staged rows").unwrap();
        let dispatch = if expect_unknown {
            Dispatch::Unknown
        } else {
            Dispatch::ConflictSequence(vec![source.clone()])
        };
        let publisher = ControlledPublisher::new(&fixture, source, dispatch);
        let report = run(
            &fixture.operation,
            &intent,
            &PreparedChange::default(),
            &[&CompactFastAppendPreparer(fixture.operation.clone())],
            &publisher,
            RetryPolicy::from_properties(&HashMap::from([(
                "commit.retry.num-retries".into(),
                "0".into(),
            )]))
            .unwrap(),
        )
        .await;
        assert_eq!(publisher.requests.lock().unwrap().len(), 1, "{report:?}");
        match report.publication {
            PublicationOutcome::KnownUncommitted(failure) if !expect_unknown => {
                assert_eq!(failure.failure_fact().reason(), COMMIT_TRANSACTION_CONFLICT);
                assert!(failure.message().contains("after 1 commit attempts"));
                assert!(!retained.exists());
            }
            PublicationOutcome::Unknown { failure, .. } if expect_unknown => {
                assert_eq!(
                    failure.failure_fact().reason(),
                    ICEBERG_COMMIT_STATE_UNKNOWN
                );
                assert_eq!(report.cleanup, IcebergCleanupReport::NotAttempted);
                assert!(retained.exists());
            }
            other => panic!("unexpected verdict: {other:?}"),
        }
    }
}

struct FormatGuard {
    calls: AtomicUsize,
    source: FormatVersion,
}

#[async_trait]
impl Preparer for FormatGuard {
    async fn prepare(
        &self,
        view: &crate::commit::staging::StagedView<'_>,
        _intent: &OperationIntent,
    ) -> Result<PreparedChange> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        crate::commit::new_manifests::validate_position_format(
            self.source,
            view.metadata().format_version(),
        )?;
        Ok(PreparedChange::default())
    }
}

#[tokio::test]
async fn window_and_format_failures_dispatch_nothing_and_clean_owned_objects() {
    for isolation in [IsolationLevel::Serializable, IsolationLevel::Snapshot] {
        let seed = Fixture::new();
        let base = append_state(&seed, seed.metadata(FormatVersion::V2), 2).await;
        let upgraded = base
            .clone()
            .into_builder(None)
            .upgrade_format_version(FormatVersion::V3)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let winner = Fixture::new();
        let advanced = append_state(&winner, upgraded, 4).await;
        let fixture = Fixture::new();
        let retained = fixture.directory.path().join("received.parquet");
        std::fs::write(&retained, b"received staged rows").unwrap();
        fixture
            .operation
            .adopt_session_data(
                ObjectIdentity::new(format!("file://{}", retained.display())).unwrap(),
            )
            .unwrap();
        let intent = row_delta_intent(&fixture, &base, isolation);
        let publisher = ControlledPublisher::new(&fixture, advanced, Dispatch::Commit);
        let guard = FormatGuard {
            calls: AtomicUsize::new(0),
            source: FormatVersion::V2,
        };
        let report = run(
            &fixture.operation,
            &intent,
            &PreparedChange::default(),
            &[&guard],
            &publisher,
            policy(),
        )
        .await;
        let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
            panic!("expected a preparation rejection: {report:?}");
        };
        assert_eq!(
            failure.failure_fact().reason(),
            ICEBERG_COMMIT_VALIDATION_CONFLICT
        );
        match isolation {
            IsolationLevel::Serializable => {
                assert!(failure.message().starts_with("Found conflicting files"));
                assert_eq!(guard.calls.load(Ordering::SeqCst), 0);
            }
            IsolationLevel::Snapshot => {
                assert!(failure.message().starts_with("Must use DVs"));
                assert_eq!(guard.calls.load(Ordering::SeqCst), 1);
            }
        }
        assert!(publisher.requests.lock().unwrap().is_empty());
        assert!(matches!(
            report.cleanup,
            IcebergCleanupReport::Complete { deleted: 1 }
        ));
        assert!(!retained.exists());
    }
}
