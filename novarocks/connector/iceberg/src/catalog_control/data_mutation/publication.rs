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

//! Data mutation owns registered-file facts, never registered-file deletion.

use super::*;
use crate::catalog::error::{CatalogCommitEvidence, CatalogOutcome};
use crate::catalog::transaction::CommitProof;
use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::attempt::{self, Publisher, RetryPolicy, TransitionalPublisher};
use crate::commit::model::{
    AddedContent, ArtifactWriter, Dependency, FileChanges, FrozenRequest, IsolationLevel,
    OperationIntent, OperationIntentParts, RequestShape, TableTarget,
};
use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation, OperationLimits};
use crate::commit::staging::{PreparedChange, Preparer, StagingBase};
use async_trait::async_trait;

pub(super) fn format_error(error: ConnectorError) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, error.message())
        .with_source(error)
}

struct MutationPublisher {
    inner: TransitionalPublisher,
    planned: PlannedIcebergMutation,
    runtime: Arc<IcebergMetadataContext>,
    context: ConnectorRequestContext,
}

#[async_trait]
impl Publisher for MutationPublisher {
    async fn load_target(
        &self,
        attempt: &IcebergCommitAttempt,
    ) -> crate::iceberg::Result<StagingBase> {
        let base = self.inner.load_target(attempt).await?;
        let StagingBase::Existing {
            metadata,
            metadata_location,
        } = &base
        else {
            return Err(format_error(invalid(
                "Data mutation requires an existing target",
            )));
        };
        let table = crate::iceberg::table::Table::builder()
            .identifier(self.inner.ident.clone())
            .metadata(metadata.clone())
            .metadata_location(metadata_location.clone())
            .file_io(attempt.file_io().clone())
            .build()?;
        match &self.planned {
            PlannedIcebergMutation::Truncate { payload, .. } => {
                validate_frozen_table(&table, payload).map_err(format_error)?
            }
            PlannedIcebergMutation::RegisterExistingFiles {
                payload, manifest, ..
            } => {
                validate_add_files_target_shape(&table, payload).map_err(format_error)?;
                ConnectorOperationControl::check_active(&self.context).map_err(format_error)?;
                let source = payload
                    .source_location
                    .as_deref()
                    .ok_or_else(|| format_error(corrupt("ADD FILES source location is absent")))?;
                // This helper bridges through the injected catalog runtime. The
                // same authoritative M is validated and returned to staging.
                let revalidated = revalidate_manifest_for_table(
                    &table,
                    source,
                    &self
                        .runtime
                        .resources()
                        .planning_binding()
                        .for_request(self.context.clone()),
                    manifest,
                    self.runtime.resources().catalog_runtime(),
                    self.runtime.novarocks_catalog().listing_admission(),
                );
                ConnectorOperationControl::check_active(&self.context).map_err(format_error)?;
                revalidated.map_err(|error| format_error(map_provider_error(error)))?;
            }
        }
        attempt.check_active()?;
        Ok(base)
    }

    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> crate::iceberg::Result<()> {
        self.inner.preflight_recovery(request, operation)
    }

    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        self.inner.dispatch_once(request).await
    }
}

fn freeze_intent(
    planned: &PlannedIcebergMutation,
    operation: &IcebergCommitOperation,
    marker: &IcebergDataMutationMarkerV1,
) -> crate::iceberg::Result<OperationIntent> {
    let payload = planned.payload();
    let source = planned.source_metadata();
    let start = source_snapshot(source, &payload.target_ref).map_err(format_error)?;
    if source.uuid().to_string() != payload.table_uuid
        || start.map(|s| s.snapshot_id) != payload.base_snapshot_id
        || start.map(|s| s.sequence_number) != payload.base_sequence_number
        || source.current_schema_id() != payload.schema_id
        || source.default_partition_spec_id() != payload.default_spec_id
    {
        return Err(format_error(corrupt(
            "Data mutation lost its exact admitted source metadata",
        )));
    }
    let (added, dependencies) = match planned {
        PlannedIcebergMutation::RegisterExistingFiles { manifest, .. } => (
            manifest
                .to_data_files()
                .map_err(|error| format_error(map_provider_error(error)))?
                .into_iter()
                .map(|file| AddedContent::new_logical_data(file, payload.default_spec_id))
                .collect::<crate::iceberg::Result<Vec<_>>>()?,
            vec![Dependency::RegisteredFilesNotLive(
                manifest
                    .records
                    .iter()
                    .map(|record| record.location.clone())
                    .collect(),
            )],
        ),
        PlannedIcebergMutation::Truncate { .. } => (vec![], vec![Dependency::RefUnchanged]),
    };
    let marker_value =
        canonical_json(marker, "Iceberg data mutation marker").map_err(format_error)?;
    let summary = BTreeMap::from([(
        MARKER_PROPERTY.into(),
        String::from_utf8(marker_value.to_vec())
            .map_err(|_| format_error(internal("Canonical marker is not UTF-8")))?,
    )]);
    OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: TableIdent::new(
                NamespaceIdent::new(payload.namespace.clone()),
                payload.table.clone(),
            ),
            uuid: Some(source.uuid()),
        },
        target_ref: payload.target_ref.clone(),
        start,
        changes: FileChanges {
            added,
            removed: vec![],
        },
        dependencies,
        isolation: IsolationLevel::Serializable,
        shape: RequestShape::SnapshotProducing,
        summary,
        token: operation.token(),
    })
}

impl RegisteredIcebergDataMutationBackend {
    pub(super) fn execute_publication(
        &self,
        planned: &PlannedIcebergMutation,
        marker: &IcebergDataMutationMarkerV1,
        recovery: &MutationRecoveryTemplate,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        let operation = match IcebergCommitOperation::new(
            OperationToken::from_mutation(recovery.plan.operation_id()),
            planned.source_metadata().location(),
            self.runtime.resources().planning_binding().clone(),
            context.clone(),
            self.runtime.resources().catalog_runtime().clone(),
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
        let journal = PublicationJournal::new(operation.token());
        // External registered parquet is deliberately never adopted into this ledger.
        let setup = (|| {
            let table = self.reload_table(
                &planned.payload().namespace,
                &planned.payload().table,
                context,
            )?;
            match planned {
                PlannedIcebergMutation::Truncate { payload, .. } => {
                    validate_frozen_table(&table, payload)?
                }
                PlannedIcebergMutation::RegisterExistingFiles { payload, .. } => {
                    validate_add_files_target_shape(&table, payload)?
                }
            }
            anchor_committed_write(&self.runtime, &table)?;
            let intent = freeze_intent(planned, &operation, marker)
                .map_err(|error| corrupt(error.to_string()))?;
            let policy = RetryPolicy::from_properties(planned.source_metadata().properties())
                .map_err(|error| invalid(error.to_string()))?;
            Ok::<_, ConnectorError>((intent, policy))
        })();
        let (intent, policy) = match setup {
            Ok(setup) => setup,
            Err(error) => {
                let rejected_operation = operation.clone();
                let failure = attempt::before_dispatch_failure(&format_error(error));
                let result = operation
                    .runtime()
                    .block_on(async move { attempt::rejected(&rejected_operation, failure).await });
                return self.project_bridge(&operation, &journal, result);
            }
        };
        let preflight_journal = journal.clone();
        let template = recovery.clone();
        let preflight = Arc::new(
            move |request: &FrozenRequest, operation: &IcebergCommitOperation| {
                let facts = FrozenPublicationFacts::from_request(request, operation)?;
                let evidence = template
                    .evidence(RecoveryPublication::Dispatched { facts })
                    .map_err(format_error)?;
                let snapshot = request
                    .ref_snapshot_after(&template.payload.target_ref)
                    .ok_or_else(|| {
                        format_error(corrupt(
                            "Snapshot-producing mutation has no published snapshot",
                        ))
                    })?;
                let receipt = template.receipt(snapshot).map_err(format_error)?;
                preflight_journal.record(evidence, receipt, request)
            },
        );
        let direct = TransitionalPublisher {
            catalog: Arc::clone(self.runtime.novarocks_catalog()),
            ident: intent.target().ident.clone(),
            target_ref: intent.target_ref().into(),
            evidence: CatalogCommitEvidence::for_target(format!(
                "{}.{}",
                planned.payload().namespace,
                planned.payload().table
            ))
            .with_target_uuid(planned.payload().table_uuid.clone())
            .with_commit_uuid(marker.operation_id_hex.clone()),
            recovery_preflight: preflight,
        };
        let publisher = journal.observe(Arc::new(MutationPublisher {
            inner: direct,
            planned: planned.clone(),
            runtime: Arc::clone(&self.runtime),
            context: context.clone(),
        }));
        let prefix = PreparedChange {
            updates: vec![],
            requirements: vec![
                crate::iceberg::TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: planned.payload().schema_id,
                },
                crate::iceberg::TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: planned.payload().default_spec_id,
                },
            ],
        };
        let preparer: Box<dyn Preparer> = match planned {
            PlannedIcebergMutation::RegisterExistingFiles { .. } => {
                Box::new(crate::commit::fast_append::FastAppendPreparer)
            }
            PlannedIcebergMutation::Truncate { .. } => {
                Box::new(crate::commit::truncate::TruncatePreparer)
            }
        };
        let running_operation = operation.clone();
        let report = operation.runtime().block_on(async move {
            attempt::run(
                &running_operation,
                &intent,
                &prefix,
                &[preparer.as_ref()],
                &publisher,
                policy,
            )
            .await
        });
        let mut outcome = self.project_bridge(&operation, &journal, report)?;
        if let ExternalMutationOutcome::KnownCommitted { finalization, .. } = &mut outcome {
            if let Err(error) = self.finalize_publication(planned, context) {
                let message = match finalization {
                    ExternalMutationFinalization::Complete => error.to_string(),
                    ExternalMutationFinalization::Failed(existing) => {
                        format!("{}; {}", existing.message(), error)
                    }
                };
                *finalization = ExternalMutationFinalization::Failed(failure(
                    ConnectorMutationFailureKind::Internal,
                    message,
                ));
            }
        }
        Ok(outcome)
    }

    fn project_bridge(
        &self,
        operation: &IcebergCommitOperation,
        journal: &PublicationJournal<ConnectorDataMutationReceipt>,
        result: Result<attempt::Report, String>,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        match result {
            Ok(report) => Ok(journal.project(report)),
            Err(error) => {
                let recovery_operation = operation.clone();
                let recovery_journal = journal.clone();
                let recovered = operation.runtime().block_on(async move {
                    recovery_journal
                        .recover_bridge(&recovery_operation, error)
                        .await
                });
                Ok(project_recovery_bridge(operation, journal, recovered))
            }
        }
    }

    fn finalize_publication(
        &self,
        planned: &PlannedIcebergMutation,
        context: &ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        let payload = planned.payload();
        self.runtime
            .control_state()
            .invalidate_table_cache(&payload.namespace, &payload.table);
        let reloaded = self
            .runtime
            .load_table_for_request(&payload.namespace, &payload.table, context)
            .map_err(map_provider_error)?;
        if let PlannedIcebergMutation::RegisterExistingFiles { manifest, .. } = planned {
            let mapping = reloaded
                .table
                .metadata()
                .properties()
                .get(crate::iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
                .map(|mapping| crate::schema_mapping::canonical_name_mapping(mapping))
                .transpose()
                .map_err(map_provider_error)?;
            if mapping.as_deref() != manifest.canonical_name_mapping.as_deref() {
                return Err(corrupt(
                    "schema.name-mapping.default changed after ADD FILES commit",
                ));
            }
        }
        Ok(())
    }
}

fn project_recovery_bridge(
    operation: &IcebergCommitOperation,
    journal: &PublicationJournal<ConnectorDataMutationReceipt>,
    result: Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, String>,
) -> ExternalMutationOutcome<ConnectorDataMutationReceipt> {
    match result {
        Ok(outcome) => outcome,
        Err(error) => journal.bridge_failure(
            operation,
            format!("Iceberg mutation recovery bridge: {error}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_control::data_mutation::tests::{
        exact_provider_with_empty_table, register_plan, table_context, write_external_parquet,
    };
    use crate::commit::model::{ArtifactClass, ArtifactKind};
    use novarocks_spi::connector::ConnectorMetadata;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy)]
    enum Behavior {
        Conflict,
        Commit,
        Unknown,
        PanicBeforeLoad,
        PanicAfterIssue,
        Capacity(usize),
    }

    struct ControlledPublisher {
        base: StagingBase,
        template: MutationRecoveryTemplate,
        journal: PublicationJournal<ConnectorDataMutationReceipt>,
        calls: Arc<AtomicUsize>,
        checked: Arc<Mutex<Option<ExternalMutationEvidence>>>,
        behavior: Behavior,
    }

    #[async_trait]
    impl Publisher for ControlledPublisher {
        async fn load_target(
            &self,
            attempt: &IcebergCommitAttempt,
        ) -> crate::iceberg::Result<StagingBase> {
            if matches!(self.behavior, Behavior::PanicBeforeLoad) {
                panic!("injected data mutation bridge before load");
            }
            attempt.check_active()?;
            let StagingBase::Existing {
                metadata,
                metadata_location,
            } = &self.base
            else {
                unreachable!("data mutation test base")
            };
            Ok(StagingBase::Existing {
                metadata: metadata.clone(),
                metadata_location: metadata_location.clone(),
            })
        }
        fn preflight_recovery(
            &self,
            request: &FrozenRequest,
            operation: &IcebergCommitOperation,
        ) -> crate::iceberg::Result<()> {
            if let Behavior::Capacity(count) = self.behavior {
                // Operation-lifetime allocations are real ledger entries, even
                // when preparation later rejects before opening their output.
                let allocation = operation.begin_attempt()?;
                for _ in 0..count {
                    allocation.allocate(ArtifactClass::Operation, ArtifactKind::Manifest)?;
                }
            }
            let facts = FrozenPublicationFacts::from_request(request, operation)?;
            let evidence = self
                .template
                .evidence(RecoveryPublication::Dispatched { facts })
                .map_err(format_error)?;
            let receipt = self
                .template
                .receipt(request.ref_snapshot_after(request.target_ref()).unwrap())
                .map_err(format_error)?;
            *self.checked.lock().unwrap() = Some(evidence.clone());
            self.journal.record(evidence, receipt, request)
        }
        async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                Behavior::Commit => CatalogOutcome::committed(
                    CommitProof::applied(request.ref_snapshot_after(request.target_ref())),
                    ExternalMutationEffect::Applied,
                ),
                Behavior::Conflict => CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::Conflict,
                    "injected OCC refusal",
                ),
                Behavior::Unknown => CatalogOutcome::unknown(
                    "injected lost response",
                    CatalogCommitEvidence::for_target("db.t"),
                ),
                Behavior::PanicAfterIssue => panic!("injected data mutation bridge after issue"),
                Behavior::PanicBeforeLoad | Behavior::Capacity(_) => {
                    panic!("undispatched mutation unexpectedly issued")
                }
            }
        }
    }

    fn exercise(behavior: Behavior, truncate: bool) {
        let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
        let source = tempfile::tempdir().unwrap();
        let parquet = write_external_parquet(source.path(), vec![1, 2, 3]);
        let before = std::fs::read(&parquet).unwrap();
        let adapter = IcebergDataMutationAdapter::try_new(provider.clone()).unwrap();
        let plan = if truncate {
            let metadata = provider
                .load_table(novarocks_spi::connector::ConnectorTableRequest {
                    table: novarocks_spi::connector::ConnectorTableIdentity {
                        instance_id: provider.descriptor().instance_id.clone(),
                        namespace: "db".into(),
                        table: "t".into(),
                    },
                    resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                    context: table_context(),
                })
                .unwrap();
            adapter
                .plan_mutation(
                    ConnectorDataMutationPlanningRequest::try_new(
                        ConnectorMutationOperationId::new(),
                        adapter.binding_key().clone(),
                        ConnectorDataMutationOperation::truncate(metadata.table, "main").unwrap(),
                        table_context(),
                    )
                    .unwrap(),
                )
                .unwrap()
        } else {
            register_plan(&adapter, &provider, source.path())
        };
        let cached = adapter
            .plans
            .lock()
            .unwrap()
            .get(&plan.operation_id())
            .unwrap()
            .clone();
        let marker = adapter.marker(&plan, cached.private.payload());
        let template = MutationRecoveryTemplate::new(
            adapter.descriptor.clone(),
            adapter.key.clone(),
            plan.clone(),
            cached.private.payload().clone(),
        );
        let runtime = provider.runtime();
        let table = runtime
            .load_table_for_request("db", "t", &table_context())
            .unwrap()
            .into_table();
        let base = StagingBase::Existing {
            metadata: table.metadata().clone(),
            metadata_location: table.metadata_location().unwrap().into(),
        };
        let operation = IcebergCommitOperation::new(
            OperationToken::from_mutation(plan.operation_id()),
            table.metadata().location(),
            runtime.resources().planning_binding().clone(),
            table_context(),
            runtime.resources().catalog_runtime().clone(),
            OperationLimits::default(),
        )
        .unwrap();
        let journal = PublicationJournal::new(operation.token());
        let calls = Arc::new(AtomicUsize::new(0));
        let checked = Arc::new(Mutex::new(None));
        let publisher = journal.observe(Arc::new(ControlledPublisher {
            base,
            template,
            journal: journal.clone(),
            calls: calls.clone(),
            checked: checked.clone(),
            behavior,
        }));
        let intent = freeze_intent(&cached.private, &operation, &marker).unwrap();
        let policy = RetryPolicy::from_properties(&HashMap::from([
            ("commit.retry.num-retries".into(), "1".into()),
            ("commit.retry.min-wait-ms".into(), "0".into()),
            ("commit.retry.max-wait-ms".into(), "0".into()),
        ]))
        .unwrap();
        let preparer: Box<dyn Preparer> = if truncate {
            Box::new(crate::commit::truncate::TruncatePreparer)
        } else {
            Box::new(crate::commit::fast_append::FastAppendPreparer)
        };
        let running = operation.clone();
        let result = operation.runtime().block_on(async move {
            attempt::run(
                &running,
                &intent,
                &PreparedChange::default(),
                &[preparer.as_ref()],
                &publisher,
                policy,
            )
            .await
        });
        let backend = RegisteredIcebergDataMutationBackend::new(provider.clone());
        let outcome = backend
            .project_bridge(&operation, &journal, result)
            .unwrap();
        assert_eq!(std::fs::read(&parquet).unwrap(), before);
        assert!(
            operation
                .artifacts()
                .unwrap()
                .iter()
                .all(|record| record.class != ArtifactClass::SessionData
                    && record.object.path() != format!("file://{}", parquet.display()))
        );
        match behavior {
            Behavior::Unknown | Behavior::PanicAfterIssue => {
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                let ExternalMutationOutcome::CommitUnknown { evidence, .. } = outcome else {
                    panic!("issued mutation lost its unknown verdict")
                };
                let payload: IcebergDataMutationEvidenceV2 =
                    decode_canonical_json(evidence.provider_payload(), "actual recovery").unwrap();
                assert!(matches!(
                    &payload.publication,
                    RecoveryPublication::Dispatched { .. }
                ));
                validate_recovery_facts(&payload, plan.operation_id()).unwrap();
                let records = operation.artifacts().unwrap();
                assert!(!records.is_empty());
                for record in records {
                    assert!(
                        std::path::Path::new(record.object.path().strip_prefix("file://").unwrap())
                            .exists()
                    );
                }
                assert_eq!(evidence, checked.lock().unwrap().clone().unwrap());
                let before = operation.recovery_artifacts();
                let secondary = project_recovery_bridge(
                    &operation,
                    &journal,
                    Err("injected second bridge failure".into()),
                );
                let ExternalMutationOutcome::CommitUnknown {
                    evidence: secondary,
                    ..
                } = secondary
                else {
                    panic!("secondary bridge lost its exact issued carrier")
                };
                assert_eq!(secondary, evidence);
                assert_eq!(operation.recovery_artifacts(), before);
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
            Behavior::Commit => {
                let ExternalMutationOutcome::KnownCommitted {
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                    ..
                } = outcome
                else {
                    panic!("controlled mutation lost its commit proof")
                };
                let first = backend
                    .project_bridge(
                        &operation,
                        &journal,
                        Err("injected first post-proof bridge failure".into()),
                    )
                    .unwrap();
                let second = project_recovery_bridge(
                    &operation,
                    &journal,
                    Err("injected second recovery bridge failure".into()),
                );
                for outcome in [first, second] {
                    let ExternalMutationOutcome::KnownCommitted {
                        receipt: preserved,
                        finalization: ExternalMutationFinalization::Failed(_),
                        ..
                    } = outcome
                    else {
                        panic!("post-proof bridge failure downgraded a proven commit")
                    };
                    assert_eq!(preserved, receipt);
                }
                for record in operation.recovery_artifacts() {
                    assert!(
                        std::path::Path::new(record.object.path().trim_start_matches("file://"))
                            .exists()
                    );
                }
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
            Behavior::Conflict => {
                assert_eq!(calls.load(Ordering::SeqCst), 2);
                assert!(
                    matches!(outcome, ExternalMutationOutcome::KnownUncommitted { failure, cleanup: ExternalMutationFinalization::Complete } if failure.kind() == ConnectorMutationFailureKind::Conflict)
                );
                assert!(operation.artifacts().unwrap().is_empty());
            }
            Behavior::PanicBeforeLoad => {
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                assert!(matches!(
                    outcome,
                    ExternalMutationOutcome::KnownUncommitted {
                        cleanup: ExternalMutationFinalization::Complete,
                        ..
                    }
                ));
                assert!(operation.artifacts().unwrap().is_empty());
            }
            Behavior::Capacity(_) => {
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                assert!(
                    matches!(outcome, ExternalMutationOutcome::KnownUncommitted { failure, cleanup: ExternalMutationFinalization::Complete } if failure.kind() == ConnectorMutationFailureKind::ResourceExhausted)
                );
                assert!(operation.artifacts().unwrap().is_empty());
            }
        }
    }

    #[test]
    fn add_files_retry_exhaustion_cleans_only_owned_metadata() {
        exercise(Behavior::Conflict, false);
    }

    #[test]
    fn add_files_bridge_frontier_preserves_external_files_and_actual_recovery() {
        for behavior in [
            Behavior::PanicBeforeLoad,
            Behavior::PanicAfterIssue,
            Behavior::Unknown,
        ] {
            exercise(behavior, false);
        }
    }

    #[test]
    fn mutation_actual_full_ledger_capacity_refuses_before_dispatch() {
        exercise(Behavior::Capacity(400), false);
        exercise(Behavior::Capacity(50), true);
    }

    #[test]
    fn mutation_secondary_recovery_bridge_keeps_exact_unknown_or_committed_outcome() {
        for truncate in [false, true] {
            exercise(Behavior::PanicAfterIssue, truncate);
            exercise(Behavior::Commit, truncate);
        }
    }
    struct CountedSourcePublisher {
        inner: MutationPublisher,
        dispatches: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Publisher for CountedSourcePublisher {
        async fn load_target(
            &self,
            attempt: &IcebergCommitAttempt,
        ) -> crate::iceberg::Result<StagingBase> {
            self.inner.load_target(attempt).await
        }
        fn preflight_recovery(
            &self,
            request: &FrozenRequest,
            operation: &IcebergCommitOperation,
        ) -> crate::iceberg::Result<()> {
            self.inner.preflight_recovery(request, operation)
        }
        async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            self.inner.dispatch_once(request).await
        }
    }

    fn source_stop_after_issued_request(deadline: bool, tail: bool) {
        use crate::access_binding::IcebergReadBinding;
        use crate::catalog_control::add_files::source_control_tests::SourceHttpFixture;
        use crate::resources::IcebergMetadataResources;
        use novarocks_spi::connector::{
            ConnectorMutationFailureKind, ConnectorStopOwner, ConnectorTableIdentity,
            ConnectorTableRequest, ConnectorTableResolution,
        };
        use std::time::{Duration, Instant};

        let (executor, _warehouse, original_provider) = exact_provider_with_empty_table();
        let external = tempfile::tempdir().unwrap();
        let parquet = write_external_parquet(external.path(), vec![1, 2, 3]);
        let original_bytes = std::fs::read(&parquet).unwrap();
        let source = SourceHttpFixture::new(original_bytes.clone());
        let binding = IcebergReadBinding::new(
            Some(source.config()),
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let source_runtime = Arc::new(
            IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(
                    original_provider
                        .runtime()
                        .control_state()
                        .configuration()
                        .clone(),
                ),
                IcebergMetadataResources::new(binding, executor.handle().clone()),
            )
            .unwrap(),
        );
        // The exact Hadoop target and the HTTP source have separate storage
        // bindings. Only source revalidation uses the S3-only runtime.
        let provider = original_provider;
        let runtime = provider.runtime().clone();
        let adapter = IcebergDataMutationAdapter::try_new(provider.clone()).unwrap();
        let local_plan = register_plan(&adapter, &provider, external.path());
        let mut planned = adapter
            .plans
            .lock()
            .unwrap()
            .get(&local_plan.operation_id())
            .unwrap()
            .private
            .clone();
        let before = runtime
            .load_table_for_request("db", "t", &table_context())
            .unwrap()
            .into_table();
        let source_binding = source_runtime
            .resources()
            .planning_binding()
            .for_request(table_context());
        let source_manifest = plan_manifest_for_table(
            &before,
            source.source(),
            &source_binding,
            source_runtime.resources().catalog_runtime(),
            source_runtime.novarocks_catalog().listing_admission(),
        )
        .unwrap();
        let PlannedIcebergMutation::RegisterExistingFiles {
            payload,
            manifest,
            domain,
            ..
        } = &mut planned
        else {
            unreachable!()
        };
        // The domain digest is solely the exact protected Hadoop roots, not
        // the source path. Those roots were proved by the real local plan;
        // this S3 source is in a distinct physical authority. No digest is
        // invented: the new state/scope come from actual HTTP source reads.
        payload.source_location = Some(source.source().to_owned());
        payload.name_mapping_digest_hex = source_manifest
            .canonical_name_mapping
            .as_deref()
            .map(|mapping| hex_encode(Sha256::digest(mapping.as_bytes())));
        *manifest = source_manifest;
        let resolved = provider
            .load_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: "db".into(),
                    table: "t".into(),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: table_context(),
            })
            .unwrap();
        let request = ConnectorDataMutationPlanningRequest::try_new(
            ConnectorMutationOperationId::new(),
            adapter.binding_key().clone(),
            ConnectorDataMutationOperation::register_existing_files(
                resolved.table,
                source.source(),
            )
            .unwrap(),
            table_context(),
        )
        .unwrap();
        let plan = ConnectorDataMutationPlan::try_new(
            &request,
            manifest.digest,
            ConnectorDataMutationPlanSummary::try_new(
                manifest.records.len().try_into().unwrap(),
                manifest.total_rows,
                manifest.total_bytes,
            )
            .unwrap(),
            Some(manifest.source_scope),
            Some(*domain),
            canonical_json(payload, "Iceberg data mutation plan").unwrap(),
        )
        .unwrap();
        let marker = adapter.marker(&plan, planned.payload());
        let before_location = before.metadata_location().unwrap().to_string();
        let before_metadata = serde_json::to_value(before.metadata()).unwrap();
        let stop = ConnectorStopOwner::new();
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(if deadline { 2 } else { 30 }),
            stop.view(),
            64 * 1024,
            256 * 1024,
        )
        .unwrap();
        let operation = IcebergCommitOperation::new(
            OperationToken::from_mutation(plan.operation_id()),
            before.metadata().location(),
            runtime.resources().planning_binding().clone(),
            context.clone(),
            runtime.resources().catalog_runtime().clone(),
            OperationLimits::default(),
        )
        .unwrap();
        let intent = freeze_intent(&planned, &operation, &marker).unwrap();
        let dispatches = Arc::new(AtomicUsize::new(0));
        let journal = PublicationJournal::<ConnectorDataMutationReceipt>::new(operation.token());
        let publisher = journal.observe(Arc::new(CountedSourcePublisher {
            inner: MutationPublisher {
                inner: TransitionalPublisher {
                    catalog: runtime.novarocks_catalog().clone(),
                    ident: intent.target().ident.clone(),
                    target_ref: "main".into(),
                    evidence: CatalogCommitEvidence::for_target("db.t"),
                    recovery_preflight: Arc::new(|_, _| {
                        panic!("stopped source must not reach preflight")
                    }),
                },
                planned: planned.clone(),
                runtime: source_runtime.clone(),
                context: context.clone(),
            },
            dispatches: dispatches.clone(),
        }));
        let policy = RetryPolicy::from_properties(before.metadata().properties()).unwrap();
        if tail {
            source.arm_tail();
        } else {
            source.arm_stat();
        }
        let running = operation.clone();
        let bridge = operation.runtime().clone();
        let worker = std::thread::spawn(move || {
            bridge.block_on(async move {
                attempt::run(
                    &running,
                    &intent,
                    &PreparedChange::default(),
                    &[&crate::commit::fast_append::FastAppendPreparer],
                    &publisher,
                    policy,
                )
                .await
            })
        });
        source.wait_for_tail();
        if deadline {
            std::thread::sleep(
                context.deadline().saturating_duration_since(Instant::now())
                    + Duration::from_millis(5),
            );
        } else {
            stop.request_stop();
        }
        let expected = if deadline {
            ConnectorMutationFailureKind::DeadlineExceeded
        } else {
            ConnectorMutationFailureKind::Cancelled
        };
        assert_eq!(
            attempt::before_dispatch_failure(&format_error(context.check_active().unwrap_err()))
                .kind(),
            expected
        );
        assert!(
            !worker.is_finished(),
            "issued source request must reach actual completion before operation exit"
        );
        source.release_tail();
        let report = worker.join().unwrap().unwrap();
        let backend = RegisteredIcebergDataMutationBackend::new(provider);
        let outcome = backend
            .project_bridge(&operation, &journal, Ok(report))
            .unwrap();
        let ExternalMutationOutcome::KnownUncommitted { failure, cleanup } = outcome else {
            panic!("stopped source must be definitely unpublished")
        };
        assert_eq!(failure.kind(), expected);
        assert_eq!(cleanup, ExternalMutationFinalization::Complete);
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        assert!(
            operation.artifacts().unwrap().is_empty(),
            "external registration never grants ownership"
        );
        let requests = source.requests();
        let reads = requests
            .iter()
            .filter(|request| {
                request.method == "GET"
                    && request
                        .target
                        .split('?')
                        .next()
                        .unwrap()
                        .ends_with(".parquet")
            })
            .collect::<Vec<_>>();
        if tail {
            assert_eq!(
                reads.len(),
                1,
                "no footer body or subsequent file read: {requests:?}"
            );
            assert!(reads[0].target.contains("a.parquet"));
            assert_eq!(
                reads[0].range.as_deref(),
                Some(source.tail_range().as_str())
            );
        } else {
            assert!(
                reads.is_empty(),
                "no footer reads after issued stat returns: {requests:?}"
            );
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.method == "HEAD")
                    .count(),
                1,
                "no subsequent stat: {requests:?}"
            );
        }
        assert!(
            requests
                .iter()
                .all(|request| request.method == "GET" || request.method == "HEAD"),
            "external objects must not receive mutation requests"
        );
        assert_eq!(source.bytes(), original_bytes.as_slice());
        assert_eq!(std::fs::read(&parquet).unwrap(), original_bytes);
        let after = runtime
            .load_table_for_request("db", "t", &table_context())
            .unwrap()
            .into_table();
        assert_eq!(after.metadata_location(), Some(before_location.as_str()));
        assert_eq!(
            serde_json::to_value(after.metadata()).unwrap(),
            before_metadata
        );
    }

    #[test]
    fn add_files_revalidation_stop_while_tail_is_issued_prevents_next_read_and_dispatch() {
        source_stop_after_issued_request(false, true);
    }
    #[test]
    fn add_files_revalidation_deadline_while_tail_is_issued_prevents_next_read_and_dispatch() {
        source_stop_after_issued_request(true, true);
    }
    #[test]
    fn add_files_revalidation_stop_while_stat_is_issued_waits_before_refusing_next_request() {
        source_stop_after_issued_request(false, false);
    }
    #[test]
    fn add_files_revalidation_deadline_while_stat_is_issued_waits_before_refusing_next_request() {
        source_stop_after_issued_request(true, false);
    }
}
