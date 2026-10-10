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

//! One measured statistics intent, prepared on fresh authoritative metadata.

use super::*;
use crate::catalog::error::CatalogCommitEvidence;
use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::attempt::{
    self, OwnerPublisher, Publisher, RecoveryPreflight, RetryPolicy, TransitionalPublisher,
};
use crate::commit::model::{
    ArtifactWriter, BaseIdentity, Dependency, FileChanges, FrozenRequest, IsolationLevel,
    OperationIntent, OperationIntentParts, OperationToken, RequestShape, StartSnapshot,
    TableTarget,
};
use crate::commit::operation::{IcebergCommitOperation, OperationLimits};
use crate::commit::recovery::FrozenPublicationFacts;
use crate::commit::staging::{PreparedChange, Preparer, StagedView};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

fn preparation_error(error: ConnectorError) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, error.message())
        .with_source(error)
}

/// Measurements and aggregate bodies never change when catalog metadata moves.
struct StatisticsDraftPreparer {
    snapshot_id: i64,
    sequence_number: i64,
    drafts: Vec<StatisticsArtifactDraft>,
}

#[async_trait]
impl Preparer for StatisticsDraftPreparer {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        _intent: &OperationIntent,
    ) -> crate::iceberg::Result<PreparedChange> {
        self.prepare_artifacts(view.artifacts()).await
    }
}

impl StatisticsDraftPreparer {
    async fn prepare_artifacts(
        &self,
        writer: &dyn ArtifactWriter,
    ) -> crate::iceberg::Result<PreparedChange> {
        writer.check_active()?;
        let written = crate::stats_assembler::write_puffin_artifacts_allocated(
            writer,
            self.snapshot_id,
            self.sequence_number,
            &self.drafts,
        )
        .await;
        // Keep a typed late cancellation even if the writer's String result
        // also reports it. The operation waits for actual writer exit.
        writer.check_active()?;
        let statistics = written
            .map_err(|error| preparation_error(unavailable(error)))?
            .ok_or_else(|| {
                preparation_error(corrupt(
                    "Non-empty statistics drafts produced no Puffin file",
                ))
            })?;
        Ok(PreparedChange {
            requirements: vec![],
            updates: vec![crate::iceberg::TableUpdate::SetStatistics { statistics }],
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatisticsRecovery {
    version: u16,
    measured_snapshot_id: i64,
    measured_sequence_number: i64,
    data_version: Vec<u8>,
    evidence_revision: Vec<u8>,
    statistics_path: String,
    publication: FrozenPublicationFacts,
}

#[derive(Clone)]
struct Projection {
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    incarnation: novarocks_spi::connector::ProviderBindingEpoch,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    target: crate::iceberg::TableIdent,
    uuid: uuid::Uuid,
    snapshot_id: i64,
    sequence_number: i64,
    data_version: StatisticsDataVersion,
    revision: StatisticsEvidenceRevision,
}

impl Projection {
    fn intent(&self) -> crate::iceberg::Result<OperationIntent> {
        OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: self.target.clone(),
                uuid: Some(self.uuid),
            },
            target_ref: "main".into(),
            start: Some(StartSnapshot {
                snapshot_id: self.snapshot_id,
                sequence_number: self.sequence_number,
            }),
            changes: FileChanges::default(),
            dependencies: vec![Dependency::MeasuredSnapshotExists(self.snapshot_id)],
            isolation: IsolationLevel::Serializable,
            shape: RequestShape::MetadataOnly,
            summary: BTreeMap::new(),
            token: OperationToken::from_mutation(self.operation_id),
        })
    }

    fn preflight(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
        journal: &PublicationJournal<StatisticsReceipt>,
    ) -> crate::iceberg::Result<()> {
        if request.shape() != RequestShape::MetadataOnly
            || request.identifier() != &self.target
            || request.target_ref() != "main"
            || operation.token() != OperationToken::from_mutation(self.operation_id)
            || !matches!(request.base(), BaseIdentity::Existing { uuid, .. } if *uuid == self.uuid)
        {
            return Err(preparation_error(invalid(
                "Statistics frozen request differs from its original measurement target",
            )));
        }
        let [crate::iceberg::TableUpdate::SetStatistics { statistics }] = request.updates() else {
            return Err(preparation_error(corrupt(
                "Statistics publication must contain exactly one SetStatistics update",
            )));
        };
        if statistics.snapshot_id != self.snapshot_id {
            return Err(preparation_error(corrupt(
                "Statistics publication changed its measured snapshot",
            )));
        }
        let payload = StatisticsRecovery {
            version: STATISTICS_PUBLICATION_EVIDENCE_VERSION,
            measured_snapshot_id: self.snapshot_id,
            measured_sequence_number: self.sequence_number,
            data_version: self.data_version.as_bytes().to_vec(),
            evidence_revision: self.revision.as_bytes().to_vec(),
            statistics_path: statistics.statistics_path.clone(),
            publication: FrozenPublicationFacts::from_request(request, operation)?,
        };
        let bytes = serde_json::to_vec(&payload).map_err(|error| {
            preparation_error(internal(format!(
                "Encode complete statistics recovery ledger: {error}"
            )))
        })?;
        let evidence = ExternalMutationEvidence::try_new(
            STATISTICS_PUBLICATION_EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.incarnation,
            self.operation_id,
            STATISTICS_OPERATION_KIND,
            Bytes::from(bytes),
        )
        .map_err(preparation_error)?;
        let receipt = StatisticsReceipt::try_new(
            self.descriptor.clone(),
            self.incarnation,
            self.operation_id,
            self.data_version.clone(),
            self.revision.clone(),
            Bytes::from(statistics.statistics_path.clone()),
        )
        .map_err(preparation_error)?;
        journal.record(evidence, receipt, request)
    }

    fn preflight_callback(
        &self,
        journal: &PublicationJournal<StatisticsReceipt>,
    ) -> RecoveryPreflight {
        let projection = self.clone();
        let journal = journal.clone();
        Arc::new(move |request, operation| projection.preflight(request, operation, &journal))
    }
}

/// The runner and the journal own every transition after output allocation.
fn run_publication(
    operation: &IcebergCommitOperation,
    intent: OperationIntent,
    preparer: Arc<dyn Preparer>,
    publisher: Arc<dyn Publisher>,
    journal: &PublicationJournal<StatisticsReceipt>,
    policy: RetryPolicy,
) -> ExternalMutationOutcome<StatisticsReceipt> {
    let observed = journal.observe(publisher);
    let run_operation = operation.clone();
    match operation.runtime().block_on(async move {
        attempt::run(
            &run_operation,
            &intent,
            &PreparedChange::default(),
            &[preparer.as_ref()],
            &observed,
            policy,
        )
        .await
    }) {
        Ok(report) => journal.project(report),
        Err(error) => {
            let operation_for_recovery = operation.clone();
            let recovery = journal.clone();
            match operation.runtime().block_on(async move {
                recovery
                    .recover_bridge(
                        &operation_for_recovery,
                        format!("Statistics publication runtime bridge failed: {error}"),
                    )
                    .await
            }) {
                Ok(outcome) => outcome,
                // Recovery reads the journal before awaiting cleanup. A second
                // bridge failure must still preserve any observed proof.
                Err(error) => journal.bridge_failure(
                    operation,
                    format!("Statistics publication recovery bridge failed: {error}"),
                ),
            }
        }
    }
}

pub(super) fn publish(
    session: IcebergStatisticsCollectionSession,
    drafts: Vec<StatisticsArtifactDraft>,
    revision: StatisticsEvidenceRevision,
) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
    let snapshot_id = session
        .snapshot_id
        .ok_or_else(|| corrupt("Statistics session has no measured snapshot"))?;
    let sequence_number = session
        .sequence_number
        .ok_or_else(|| corrupt("Statistics session has no measured sequence"))?;
    let projection = Projection {
        descriptor: session.provider.descriptor().clone(),
        incarnation: session.provider.incarnation(),
        operation_id: session.operation_id,
        target: session.physical_table.identifier().clone(),
        uuid: session.physical_table.metadata().uuid(),
        snapshot_id,
        sequence_number,
        data_version: session.data_version,
        revision,
    };
    let intent = projection
        .intent()
        .map_err(|error| corrupt(error.to_string()))?;
    let policy = match RetryPolicy::from_properties(session.physical_table.metadata().properties())
    {
        Ok(policy) => policy,
        Err(error) => return Ok(known_uncommitted(invalid(error.to_string()))),
    };
    let operation = match IcebergCommitOperation::new(
        intent.token(),
        session.physical_table.metadata().location(),
        session
            .provider
            .runtime()
            .resources()
            .planning_binding()
            .clone(),
        session.context,
        session
            .provider
            .runtime()
            .resources()
            .catalog_runtime()
            .clone(),
        OperationLimits::default(),
    ) {
        Ok(operation) => operation,
        Err(error) => {
            return Ok(ExternalMutationOutcome::KnownUncommitted {
                failure: attempt::before_dispatch_failure(&error),
                cleanup: ExternalMutationFinalization::Complete,
            });
        }
    };
    #[cfg(test)]
    STATISTICS_TRANSACTION_ADMISSIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let journal = PublicationJournal::new(operation.token());
    let direct = TransitionalPublisher {
        catalog: Arc::clone(session.provider.runtime().novarocks_catalog()),
        ident: projection.target.clone(),
        target_ref: "main".into(),
        evidence: CatalogCommitEvidence::for_target(format!(
            "{}.{}",
            session.table_payload.namespace, session.table_payload.table
        ))
        .with_target_uuid(projection.uuid.to_string())
        .with_target_ref("main")
        .with_commit_uuid(operation.token().to_string()),
        recovery_preflight: projection.preflight_callback(&journal),
    };
    let owner = Arc::new(OwnerPublisher {
        target: direct,
        operation: operation.token(),
        marker: None,
    });
    let preparer = Arc::new(StatisticsDraftPreparer {
        snapshot_id,
        sequence_number,
        drafts,
    });
    let outcome = run_publication(&operation, intent, preparer, owner, &journal, policy);
    if matches!(outcome, ExternalMutationOutcome::KnownCommitted { .. }) {
        session.provider.runtime().control_state().invalidate_table(
            &session.table_payload.namespace,
            &session.table_payload.table,
        );
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog::error::CatalogOutcome;
    use crate::catalog::transaction::CommitProof;
    use crate::commit::operation::IcebergCommitAttempt;
    use crate::commit::staging::StagingBase;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SortOrder, Summary, TableMetadata, TableMetadataBuilder,
    };
    use crate::resources::IcebergCatalogRuntime;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ConnectorRequestContext, ConnectorStopOwner, ProviderBindingEpoch,
    };
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct Rig {
        _runtime: tokio::runtime::Runtime,
        directory: tempfile::TempDir,
        operation: IcebergCommitOperation,
        stop: ConnectorStopOwner,
        metadata: TableMetadata,
        projection: Projection,
    }

    impl Rig {
        fn new() -> Self {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let location = format!("file://{}", directory.path().display());
            let binding = IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                    runtime.handle().clone(),
                )),
                Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                    runtime.handle().clone(),
                )),
            );
            let stop = ConnectorStopOwner::new();
            let operation_id = ConnectorMutationOperationId::new();
            let operation = IcebergCommitOperation::new(
                OperationToken::from_mutation(operation_id),
                &location,
                binding,
                ConnectorRequestContext::try_new(
                    Instant::now() + Duration::from_secs(30),
                    stop.view(),
                    1024 * 1024,
                    1024 * 1024,
                )
                .unwrap(),
                IcebergCatalogRuntime::new(runtime.handle().clone()),
                OperationLimits::default(),
            )
            .unwrap();
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
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
                    ("commit.retry.min-wait-ms".into(), "1".into()),
                    ("commit.retry.max-wait-ms".into(), "2".into()),
                    ("commit.retry.total-timeout-ms".into(), "10000".into()),
                ]),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            let metadata = append_snapshot(metadata, 7);
            let projection = Projection {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: ConnectorInstanceId::parse("statistics-operation-test").unwrap(),
                },
                incarnation: ProviderBindingEpoch::from_bytes([4; 16]),
                operation_id,
                target: crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
                uuid: metadata.uuid(),
                snapshot_id: 7,
                sequence_number: 1,
                data_version: StatisticsDataVersion::try_new(Bytes::from_static(
                    b"measured-snapshot-7",
                ))
                .unwrap(),
                revision: StatisticsEvidenceRevision::try_new(Bytes::from_static(b"revision-7"))
                    .unwrap(),
            };
            Self {
                _runtime: runtime,
                directory,
                operation,
                stop,
                metadata,
                projection,
            }
        }

        fn preparer(&self) -> Arc<dyn Preparer> {
            Arc::new(StatisticsDraftPreparer {
                snapshot_id: 7,
                sequence_number: 1,
                drafts: drafts(),
            })
        }

        fn publisher(&self, metadata: TableMetadata, behavior: Behavior) -> Arc<TestPublisher> {
            Arc::new(TestPublisher {
                metadata: Mutex::new(metadata),
                metadata_location: format!(
                    "file://{}/metadata/base.metadata.json",
                    self.directory.path().display()
                ),
                projection: self.projection.clone(),
                journal: PublicationJournal::new(self.operation.token()),
                behavior,
                dispatches: AtomicUsize::new(0),
                loads: AtomicUsize::new(0),
                paths: Mutex::new(Vec::new()),
            })
        }

        fn run(
            &self,
            publisher: Arc<TestPublisher>,
            preparer: Arc<dyn Preparer>,
        ) -> ExternalMutationOutcome<StatisticsReceipt> {
            run_publication(
                &self.operation,
                self.projection.intent().unwrap(),
                preparer,
                publisher.clone(),
                &publisher.journal,
                RetryPolicy::from_properties(self.metadata.properties()).unwrap(),
            )
        }
    }

    fn drafts() -> Vec<StatisticsArtifactDraft> {
        let body = Bytes::from_static(include_bytes!(
            "../../../../../../tests/datasketches-tck/fixtures/theta/rust_quickselect_n1000_ordered_v3.sk"
        ));
        let draft = StatisticsArtifactDraft::try_new(
            vec![1],
            APACHE_DATASKETCHES_THETA_V1,
            body,
            BTreeMap::new(),
        )
        .unwrap();
        validate_artifacts(&[draft.identity().clone()], vec![draft]).unwrap()
    }

    fn append_snapshot(metadata: TableMetadata, id: i64) -> TableMetadata {
        let snapshot = Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(metadata.current_snapshot_id())
            .with_sequence_number(metadata.next_sequence_number())
            .with_timestamp_ms(metadata.last_updated_ms())
            .with_manifest_list(format!(
                "{}/metadata/snapshot-{id}.avro",
                metadata.location()
            ))
            .with_schema_id(metadata.current_schema_id())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();
        metadata
            .into_builder(None)
            .set_branch_snapshot(snapshot, "main")
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    #[derive(Clone, Copy)]
    enum Behavior {
        Commit,
        CommitFinalizationFailed,
        Reject,
        Unknown,
        PanicAfterIssue,
        ConflictThenCommit,
        ExpireThenCommit,
        RefuseCapacity,
    }

    struct TestPublisher {
        metadata: Mutex<TableMetadata>,
        metadata_location: String,
        projection: Projection,
        journal: PublicationJournal<StatisticsReceipt>,
        behavior: Behavior,
        dispatches: AtomicUsize,
        loads: AtomicUsize,
        paths: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Publisher for TestPublisher {
        async fn load_target(
            &self,
            _: &IcebergCommitAttempt,
        ) -> crate::iceberg::Result<StagingBase> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(StagingBase::Existing {
                metadata: self.metadata.lock().unwrap().clone(),
                metadata_location: self.metadata_location.clone(),
            })
        }

        fn preflight_recovery(
            &self,
            request: &FrozenRequest,
            operation: &IcebergCommitOperation,
        ) -> crate::iceberg::Result<()> {
            // Exercise actual request ownership and provenance, not an empty
            // synthetic request with a predicted output path.
            assert_eq!(request.shape(), RequestShape::MetadataOnly);
            assert_eq!(
                request.requirements(),
                &[crate::iceberg::TableRequirement::UuidMatch {
                    uuid: self.projection.uuid
                }]
            );
            assert_eq!(request.artifacts().attempt_owned().len(), 1);
            if matches!(self.behavior, Behavior::RefuseCapacity) {
                let mut projection = self.projection.clone();
                projection.data_version =
                    StatisticsDataVersion::try_new(Bytes::from(vec![b'x'; 65500])).unwrap();
                return projection.preflight(request, operation, &self.journal);
            }
            self.projection.preflight(request, operation, &self.journal)
        }

        async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
            let dispatch = self.dispatches.fetch_add(1, Ordering::SeqCst);
            let [crate::iceberg::TableUpdate::SetStatistics { statistics }] = request.updates()
            else {
                panic!("expected exact statistics update")
            };
            assert_eq!(statistics.snapshot_id, 7);
            self.paths
                .lock()
                .unwrap()
                .push(statistics.statistics_path.clone());
            match self.behavior {
                Behavior::Unknown => {
                    return CatalogOutcome::unknown(
                        "injected lost statistics response",
                        CatalogCommitEvidence::for_target("db.t"),
                    );
                }
                Behavior::PanicAfterIssue => panic!("injected statistics bridge panic after issue"),
                Behavior::Reject => {
                    return CatalogOutcome::uncommitted(
                        ConnectorMutationFailureKind::InvalidRequest,
                        "injected definite statistics rejection",
                    );
                }
                Behavior::ConflictThenCommit if dispatch == 0 => {
                    let metadata = self.metadata.lock().unwrap().clone();
                    *self.metadata.lock().unwrap() = append_snapshot(metadata, 8);
                    return CatalogOutcome::uncommitted(
                        ConnectorMutationFailureKind::Conflict,
                        "injected statistics conflict",
                    );
                }
                _ => {}
            }
            let mut metadata = self.metadata.lock().unwrap();
            let mut builder = metadata.clone().into_builder(None);
            if matches!(self.behavior, Behavior::ExpireThenCommit) {
                builder = builder.remove_snapshots(&[7]);
            }
            *metadata = builder
                .set_statistics(statistics.clone())
                .build()
                .unwrap()
                .metadata;
            if matches!(self.behavior, Behavior::CommitFinalizationFailed) {
                return CatalogOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: CommitProof::applied(None),
                    finalization: ExternalMutationFinalization::Failed(
                        ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Unavailable,
                            "injected post-proof catalog finalization failure",
                        ),
                    ),
                };
            }
            CatalogOutcome::committed(CommitProof::applied(None), ExternalMutationEffect::Applied)
        }
    }

    fn exists(path: &str) -> bool {
        std::path::Path::new(path.strip_prefix("file://").unwrap()).exists()
    }

    #[test]
    fn statistics_current_head_movement_preserves_measured_snapshot_and_no_cas() {
        let rig = Rig::new();
        let publisher = rig.publisher(append_snapshot(rig.metadata.clone(), 8), Behavior::Commit);
        let outcome = rig.run(publisher.clone(), rig.preparer());
        let ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization: ExternalMutationFinalization::Complete,
            ..
        } = outcome
        else {
            panic!("expected committed statistics")
        };
        assert_eq!(receipt.operation_id(), rig.projection.operation_id);
        assert_eq!(receipt.data_version(), &rig.projection.data_version);
        let metadata = publisher.metadata.lock().unwrap();
        assert_eq!(metadata.current_snapshot_id(), Some(8));
        let statistics = metadata.statistics_for_snapshot(7).unwrap();
        assert!(exists(&statistics.statistics_path));
        assert_eq!(statistics.blob_metadata[0].snapshot_id, 7);
        assert_eq!(statistics.blob_metadata[0].sequence_number, 1);
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn statistics_post_proof_finalization_failure_keeps_commit_and_puffin() {
        let rig = Rig::new();
        let publisher = rig.publisher(rig.metadata.clone(), Behavior::CommitFinalizationFailed);
        let outcome = rig.run(publisher.clone(), rig.preparer());
        assert!(matches!(outcome, ExternalMutationOutcome::KnownCommitted {
            ref finalization, ..
        } if matches!(finalization, ExternalMutationFinalization::Failed(failure) if failure.message().contains("post-proof"))));
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 1);
        assert!(exists(&publisher.paths.lock().unwrap()[0]));
    }

    #[test]
    fn statistics_snapshot_expiration_after_preparation_is_an_accepted_orphan() {
        let rig = Rig::new();
        let publisher = rig.publisher(
            append_snapshot(rig.metadata.clone(), 8),
            Behavior::ExpireThenCommit,
        );
        assert!(matches!(
            rig.run(publisher.clone(), rig.preparer()),
            ExternalMutationOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        let metadata = publisher.metadata.lock().unwrap();
        assert!(metadata.snapshot_by_id(7).is_none());
        assert_eq!(metadata.current_snapshot_id(), Some(8));
        assert!(exists(
            &metadata.statistics_for_snapshot(7).unwrap().statistics_path
        ));
    }

    #[test]
    fn statistics_snapshot_expiration_before_preparation_is_known_uncommitted() {
        let rig = Rig::new();
        let metadata = append_snapshot(rig.metadata.clone(), 8)
            .into_builder(None)
            .remove_snapshots(&[7])
            .build()
            .unwrap()
            .metadata;
        let publisher = rig.publisher(metadata, Behavior::Commit);
        assert!(
            matches!(rig.run(publisher.clone(), rig.preparer()), ExternalMutationOutcome::KnownUncommitted { ref failure, cleanup: ExternalMutationFinalization::Complete } if failure.kind() == ConnectorMutationFailureKind::Conflict)
        );
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 0);
        assert!(rig.operation.artifacts().unwrap().is_empty());
    }

    #[test]
    fn statistics_unknown_retains_actual_complete_recovery_and_never_retries() {
        let rig = Rig::new();
        let publisher = rig.publisher(rig.metadata.clone(), Behavior::Unknown);
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
            rig.run(publisher.clone(), rig.preparer())
        else {
            panic!("expected unknown publication")
        };
        assert_eq!(evidence.operation_id(), rig.projection.operation_id);
        assert_eq!(
            evidence.schema_version(),
            STATISTICS_PUBLICATION_EVIDENCE_VERSION
        );
        let payload: StatisticsRecovery =
            serde_json::from_slice(evidence.provider_payload()).unwrap();
        payload.publication.validate(rig.operation.token()).unwrap();
        assert_eq!(payload.measured_snapshot_id, 7);
        assert_eq!(payload.measured_sequence_number, 1);
        assert!(exists(&payload.statistics_path));
        assert_eq!(rig.operation.artifacts().unwrap().len(), 1);
        assert_eq!(publisher.loads.load(Ordering::SeqCst), 1);
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 1);
        let round_trip: StatisticsRecovery =
            serde_json::from_slice(&serde_json::to_vec(&payload).unwrap()).unwrap();
        assert_eq!(payload.publication, round_trip.publication);
    }

    #[test]
    fn statistics_bridge_panic_after_issue_retains_puffin_and_checked_evidence() {
        let rig = Rig::new();
        let publisher = rig.publisher(rig.metadata.clone(), Behavior::PanicAfterIssue);
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
            rig.run(publisher.clone(), rig.preparer())
        else {
            panic!("expected unknown bridge outcome")
        };
        let payload: StatisticsRecovery =
            serde_json::from_slice(evidence.provider_payload()).unwrap();
        assert!(exists(&payload.statistics_path));
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(rig.operation.artifacts().unwrap().len(), 1);
    }

    #[test]
    fn statistics_definite_rejection_cleans_registered_puffin() {
        let rig = Rig::new();
        let publisher = rig.publisher(rig.metadata.clone(), Behavior::Reject);
        assert!(matches!(
            rig.run(publisher.clone(), rig.preparer()),
            ExternalMutationOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 1);
        assert!(!exists(&publisher.paths.lock().unwrap()[0]));
        assert!(rig.operation.artifacts().unwrap().is_empty());
    }

    #[test]
    fn statistics_conflict_reloads_and_allocates_a_fresh_attempt_without_rescanning() {
        let rig = Rig::new();
        let publisher = rig.publisher(rig.metadata.clone(), Behavior::ConflictThenCommit);
        assert!(matches!(
            rig.run(publisher.clone(), rig.preparer()),
            ExternalMutationOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        let paths = publisher.paths.lock().unwrap();
        assert_eq!(paths.len(), 2);
        assert_ne!(paths[0], paths[1]);
        assert!(!exists(&paths[0]));
        assert!(exists(&paths[1]));
        assert_eq!(publisher.loads.load(Ordering::SeqCst), 2);
        assert_eq!(
            publisher.metadata.lock().unwrap().current_snapshot_id(),
            Some(8)
        );
    }

    #[test]
    fn statistics_complete_recovery_capacity_refusal_is_undispatched_and_cleans() {
        let rig = Rig::new();
        let publisher = rig.publisher(rig.metadata.clone(), Behavior::RefuseCapacity);
        let outcome = rig.run(publisher.clone(), rig.preparer());
        assert!(
            matches!(outcome, ExternalMutationOutcome::KnownUncommitted { ref failure, cleanup: ExternalMutationFinalization::Complete } if failure.kind() == ConnectorMutationFailureKind::ResourceExhausted),
            "{outcome:?}"
        );
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 0);
        assert!(rig.operation.artifacts().unwrap().is_empty());
    }

    struct FailAfterClose {
        inner: StatisticsDraftPreparer,
        stop: Option<ConnectorStopOwner>,
        path: Arc<Mutex<Option<String>>>,
    }

    #[derive(Clone)]
    struct StopAtMetadata(ConnectorStopOwner);

    impl std::fmt::Debug for StopAtMetadata {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("StopAtMetadata")
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct MetadataFaultStorage {
        #[serde(skip)]
        inner: Option<crate::iceberg::io::FileIO>,
        #[serde(skip)]
        stop: Option<StopAtMetadata>,
    }

    impl MetadataFaultStorage {
        fn inner(&self) -> crate::iceberg::Result<&crate::iceberg::io::FileIO> {
            self.inner.as_ref().ok_or_else(|| {
                preparation_error(internal(
                    "Metadata fault fixture has no live operation FileIO",
                ))
            })
        }
    }

    #[typetag::serde(name = "iru5-statistics-operation-metadata-fault")]
    #[async_trait]
    impl crate::iceberg::io::Storage for MetadataFaultStorage {
        async fn exists(&self, path: &str) -> crate::iceberg::Result<bool> {
            self.inner()?.exists(path).await
        }
        async fn list_directories(&self, path: &str) -> crate::iceberg::Result<Vec<String>> {
            self.inner()?.list_directories(path).await
        }
        async fn metadata(
            &self,
            path: &str,
        ) -> crate::iceberg::Result<crate::iceberg::io::FileMetadata> {
            assert!(
                exists(path),
                "metadata fault must follow actual Puffin close"
            );
            let metadata = self.inner()?.new_input(path)?.metadata().await?;
            if let Some(stop) = &self.stop {
                stop.0.request_stop();
                Ok(metadata)
            } else {
                Err(preparation_error(unavailable(
                    "Injected Puffin metadata failure after close",
                )))
            }
        }
        async fn read(&self, path: &str) -> crate::iceberg::Result<Bytes> {
            self.inner()?.new_input(path)?.read().await
        }
        async fn reader(
            &self,
            path: &str,
        ) -> crate::iceberg::Result<Box<dyn crate::iceberg::io::FileRead>> {
            self.inner()?.new_input(path)?.reader().await
        }
        async fn write(&self, path: &str, bytes: Bytes) -> crate::iceberg::Result<()> {
            self.inner()?.new_output(path)?.write(bytes).await
        }
        async fn writer(
            &self,
            path: &str,
        ) -> crate::iceberg::Result<Box<dyn crate::iceberg::io::FileWrite>> {
            self.inner()?.new_output(path)?.writer().await
        }
        async fn delete(&self, path: &str) -> crate::iceberg::Result<()> {
            self.inner()?.delete(path).await
        }
        async fn delete_prefix(&self, path: &str) -> crate::iceberg::Result<()> {
            self.inner()?.delete_prefix(path).await
        }
        fn new_input(&self, path: &str) -> crate::iceberg::Result<crate::iceberg::io::InputFile> {
            Ok(crate::iceberg::io::InputFile::new(
                Arc::new(self.clone()),
                path.into(),
            ))
        }
        fn new_output(&self, path: &str) -> crate::iceberg::Result<crate::iceberg::io::OutputFile> {
            Ok(crate::iceberg::io::OutputFile::new(
                Arc::new(self.clone()),
                path.into(),
            ))
        }
    }

    #[typetag::serde(name = "iru5-statistics-operation-metadata-fault-factory")]
    impl crate::iceberg::io::StorageFactory for MetadataFaultStorage {
        fn build(
            &self,
            _: &crate::iceberg::io::StorageConfig,
        ) -> crate::iceberg::Result<Arc<dyn crate::iceberg::io::Storage>> {
            self.inner()?;
            Ok(Arc::new(self.clone()))
        }
    }

    struct MetadataFaultWriter<'a> {
        inner: &'a dyn ArtifactWriter,
        file_io: crate::iceberg::io::FileIO,
        path: Arc<Mutex<Option<String>>>,
    }

    impl ArtifactWriter for MetadataFaultWriter<'_> {
        fn file_io(&self) -> &crate::iceberg::io::FileIO {
            &self.file_io
        }
        fn allocate(
            &self,
            class: crate::commit::model::ArtifactClass,
            kind: crate::commit::model::ArtifactKind,
        ) -> crate::iceberg::Result<crate::commit::model::ObjectIdentity> {
            let object = self.inner.allocate(class, kind)?;
            *self.path.lock().unwrap() = Some(object.path().into());
            Ok(object)
        }
        fn attempt_token(&self) -> crate::commit::model::AttemptToken {
            self.inner.attempt_token()
        }
        fn check_active(&self) -> crate::iceberg::Result<()> {
            self.inner.check_active()
        }
        fn snapshot_artifacts(
            &self,
            references: &[crate::commit::model::ObjectIdentity],
        ) -> crate::iceberg::Result<crate::commit::model::AttemptArtifacts> {
            self.inner.snapshot_artifacts(references)
        }
    }

    #[async_trait]
    impl Preparer for FailAfterClose {
        async fn prepare(
            &self,
            view: &StagedView<'_>,
            _: &OperationIntent,
        ) -> crate::iceberg::Result<PreparedChange> {
            let file_io = crate::iceberg::io::FileIOBuilder::new(Arc::new(MetadataFaultStorage {
                inner: Some(view.artifacts().file_io().clone()),
                stop: self.stop.clone().map(StopAtMetadata),
            }))
            .build();
            let writer = MetadataFaultWriter {
                inner: view.artifacts(),
                file_io,
                path: self.path.clone(),
            };
            self.inner.prepare_artifacts(&writer).await
        }
    }

    #[test]
    fn statistics_failure_after_puffin_close_is_typed_and_cleans_complete_object() {
        let rig = Rig::new();
        let publisher = rig.publisher(rig.metadata.clone(), Behavior::Commit);
        let path = Arc::new(Mutex::new(None));
        let preparer = Arc::new(FailAfterClose {
            inner: StatisticsDraftPreparer {
                snapshot_id: 7,
                sequence_number: 1,
                drafts: drafts(),
            },
            stop: None,
            path: path.clone(),
        });
        assert!(
            matches!(rig.run(publisher.clone(), preparer), ExternalMutationOutcome::KnownUncommitted { ref failure, cleanup: ExternalMutationFinalization::Complete } if failure.kind() == ConnectorMutationFailureKind::Unavailable)
        );
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 0);
        assert!(!exists(path.lock().unwrap().as_ref().unwrap()));
    }

    #[test]
    fn statistics_cancellation_after_puffin_close_vetoes_dispatch_and_cleans() {
        let rig = Rig::new();
        let publisher = rig.publisher(rig.metadata.clone(), Behavior::Commit);
        let path = Arc::new(Mutex::new(None));
        let preparer = Arc::new(FailAfterClose {
            inner: StatisticsDraftPreparer {
                snapshot_id: 7,
                sequence_number: 1,
                drafts: drafts(),
            },
            stop: Some(rig.stop.clone()),
            path: path.clone(),
        });
        assert!(
            matches!(rig.run(publisher.clone(), preparer), ExternalMutationOutcome::KnownUncommitted { ref failure, cleanup: ExternalMutationFinalization::Complete } if failure.kind() == ConnectorMutationFailureKind::Cancelled)
        );
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 0);
        assert!(!exists(path.lock().unwrap().as_ref().unwrap()));
    }
}
