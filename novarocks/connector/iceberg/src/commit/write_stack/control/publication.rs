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

//! Freeze once from admission; every catalog conflict prepares fresh metadata.

mod lineage;

use super::*;
use crate::catalog::error::CatalogCommitEvidence;
use crate::catalog::transaction::CommitProof;
use crate::commit::attempt::{self, OwnerPublisher, Publisher, RetryPolicy, TransitionalPublisher};
use crate::commit::dependency::ValidationInputs;
use crate::commit::model::{
    AddedContent, ArtifactClass, ArtifactRecord, ArtifactWriter, BaseIdentity, CleanupScope,
    FileChanges, FrozenRequest, IcebergCleanupReport, IsolationLevel, ObjectIdentity,
    OperationIntent, OperationIntentParts, OperationToken, PublicationOutcome, RequestShape,
    StartSnapshot, TableTarget,
};
use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation, OperationLimits};
use crate::commit::staging::{PreparedChange, Preparer, StagedView, StagingBase};
use async_trait::async_trait;
use std::sync::Mutex;

fn format_error(error: ConnectorError) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, error.message())
        .with_source(error)
}

struct WriteRecipe {
    source: TableMetadata,
    files: Vec<WrittenFile>,
    cow: Option<CowUpdateRewriteSet>,
    selected: Option<crate::commit::selected_rewrite::SelectedRewriteFiles>,
    removed_dvs: BTreeSet<EntryIdentity>,
    start: Option<StartSnapshot>,
    target: TableTarget,
    target_ref: String,
    dependencies: Vec<crate::commit::model::Dependency>,
    op_kind: CommitOpKind,
    summary: BTreeMap<String, String>,
    prefix: PreparedChange,
    output_facts: crate::write_codec::IcebergWrittenOutputFacts,
}

impl WriteRecipe {
    fn freeze(
        handle: &IcebergCommitHandle,
        validated: &[ValidatedFragment<'_>],
        documents: Option<&novarocks_spi::connector::ConnectorDocumentPublicationIntent>,
    ) -> Result<Self, ConnectorError> {
        use crate::commit::model::Dependency;
        let facts = handle.table();
        let source = handle
            .source_metadata()
            .ok_or_else(|| internal("Iceberg write lost its admitted source metadata"))?
            .clone();
        let writer_metadata = handle
            .repartition()
            .map_or(&source, |r| r.prospective_metadata());
        let mut files = validated
            .iter()
            .map(|entry| written_file_from_fragment(entry.fragment, writer_metadata))
            .collect::<Result<Vec<_>, _>>()?;
        let cow = cow_update_rewrite_set(
            handle,
            &validated
                .iter()
                .map(|entry| entry.ordinal)
                .zip(files.iter().cloned())
                .collect::<Vec<_>>(),
        )?;
        if let Some(rewrite) = &cow {
            let markers = cow_output_row_ids(rewrite)?;
            for file in &mut files {
                file.first_row_id = *markers.get(&file.path).ok_or_else(|| {
                    corrupt("COW output is not owned by a frozen replacement or append branch")
                })?;
            }
            if markers.len() != files.len() {
                return Err(corrupt(
                    "COW output declarations do not exactly match staged files",
                ));
            }
        }
        let selected = selected_rewrite_files(handle);
        if crate::schema_facts::row_lineage_enabled(&source)
            && selected.as_ref().is_some_and(|rewrite| {
                rewrite.kind == crate::commit::selected_rewrite::SelectedRewriteKind::Data
            })
        {
            lineage::freeze_preserved_row_ids(validated, &mut files)?;
        }
        let removed_dvs = validated
            .iter()
            .filter(|entry| {
                matches!(
                    entry.fragment.artifact(),
                    IcebergCommitArtifact::DeletionVector(_)
                )
            })
            .flat_map(|entry| entry.fragment.merged_old_references().iter().cloned())
            .collect();
        let output_facts = crate::write_codec::written_output_facts(&files).map_err(invalid)?;
        let rows = files
            .iter()
            .filter(|file| file.content == DataContentType::Data)
            .try_fold(0u64, |sum, file| {
                sum.checked_add(file.record_count)
                    .ok_or_else(|| corrupt("Staged Iceberg row count overflow"))
            })?;
        let summary = session_snapshot_properties_with_documents(handle, rows, documents)?;
        let dependencies = vec![if matches!(
            handle.commit_op_kind(),
            CommitOpKind::FastAppend | CommitOpKind::Overwrite
        ) && handle.repartition().is_none()
            && handle.document_publication().is_none()
        {
            Dependency::NoReadDependency
        } else {
            Dependency::RefUnchanged
        }];
        let mut prefix = if let Some(repartition) = handle.repartition() {
            repartition.prepared_change().clone()
        } else {
            PreparedChange::default()
        };
        if let Some(updates) =
            publication_metadata_updates(handle.repartition(), documents, &source)?
        {
            // Repartition's declarative updates are canonicalized by StagingEngine.
            prefix.updates = updates;
        }
        // The writer schema and spec are exact admission facts, independent of the
        // ref dependency. A concurrent schema/spec change never reinterprets files.
        prefix
            .requirements
            .push(crate::iceberg::TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: facts.schema_id(),
            });
        prefix
            .requirements
            .push(crate::iceberg::TableRequirement::DefaultSpecIdMatch {
                default_spec_id: facts.default_partition_spec_id(),
            });
        let target = TableTarget {
            ident: crate::iceberg::TableIdent::from_strs([facts.namespace(), facts.table_name()])
                .map_err(|e| invalid(e.to_string()))?,
            uuid: Some(source.uuid()),
        };
        Ok(Self {
            source,
            files,
            cow,
            selected,
            removed_dvs,
            start: facts.base_snapshot_id().map(|id| StartSnapshot {
                snapshot_id: id,
                sequence_number: facts.base_sequence_number(),
            }),
            target,
            target_ref: facts.target_ref().to_string(),
            dependencies,
            op_kind: handle.commit_op_kind(),
            summary,
            prefix,
            output_facts,
        })
    }

    async fn intent(
        &self,
        operation: &IcebergCommitOperation,
    ) -> crate::iceberg::Result<OperationIntent> {
        let mut added = Vec::new();
        for file in &self.files {
            let data = crate::commit::data_file::from_written_file(file)?;
            let entry = if self.op_kind == CommitOpKind::SelectedRewrite {
                AddedContent::rewritten_data(
                    data,
                    file.partition_spec_id,
                    self.start
                        .ok_or_else(|| {
                            format_error(invalid("Rewrite requires a frozen source snapshot"))
                        })?
                        .sequence_number,
                )?
            } else {
                AddedContent::new_logical_data(data, file.partition_spec_id)?
            };
            added.push(entry);
        }
        let mut removed = Vec::new();
        if !matches!(
            self.op_kind,
            CommitOpKind::FastAppend | CommitOpKind::Overwrite | CommitOpKind::RowDelta
        ) {
            let source_io = operation.begin_attempt()?;
            let mut inputs =
                ValidationInputs::new(&self.source, self.start.map(|s| s.snapshot_id), &source_io);
            let live = inputs.live_set().await?;
            let mut identities: BTreeSet<_> = match self.op_kind {
                CommitOpKind::RowDeltaDvFromFiles => self.removed_dvs.clone(),
                CommitOpKind::CowUpdate => self
                    .cow
                    .as_ref()
                    .ok_or_else(|| format_error(invalid("COW graph is absent")))?
                    .touched_data_files
                    .iter()
                    .map(|file| EntryIdentity::DataFile {
                        path: file.old_file.clone(),
                    })
                    .collect(),
                CommitOpKind::SelectedRewrite => {
                    let selected = self
                        .selected
                        .as_ref()
                        .ok_or_else(|| format_error(invalid("Selected rewrite input is absent")))?;
                    if selected.kind == crate::commit::selected_rewrite::SelectedRewriteKind::Data {
                        selected
                            .data_paths
                            .union(&selected.delete_paths)
                            .cloned()
                            .collect()
                    } else {
                        selected.delete_paths.clone()
                    }
                }
                CommitOpKind::Truncate => live.keys().cloned().collect(),
                CommitOpKind::OverwritePartitions => live
                    .iter()
                    .filter(|(_, entry)| {
                        added.iter().any(|a| {
                            a.partition_spec_id() == entry.frozen.facts().partition_spec_id
                                && a.file().partition() == entry.file.partition()
                        })
                    })
                    .map(|(key, _)| key.clone())
                    .collect(),
                _ => BTreeSet::new(),
            };
            if matches!(
                self.op_kind,
                CommitOpKind::CowUpdate | CommitOpKind::OverwritePartitions
            ) {
                // Deletion vectors follow their exact data file, including when
                // their stored partition differs. Removing one blob grants no
                // authority to delete a shared Puffin container.
                let data_paths: BTreeSet<_> = identities
                    .iter()
                    .filter_map(|id| match id {
                        EntryIdentity::DataFile { path } => Some(path.clone()),
                        _ => None,
                    })
                    .collect();
                identities.extend(
                    live.keys()
                        .filter(|id| {
                            matches!(id,
                                EntryIdentity::DeletionVector { referenced_data_file, .. }
                                    if data_paths.contains(referenced_data_file)
                            )
                        })
                        .cloned(),
                );
            }
            for identity in identities {
                let entry = live.get(&identity).ok_or_else(|| {
                    format_error(corrupt(format!(
                        "Frozen source entry is absent: {identity:?}"
                    )))
                })?;
                removed.push(entry.frozen.clone());
            }
        }
        if self.op_kind == CommitOpKind::RowDeltaDvFromFiles
            && added.is_empty()
            && !removed.is_empty()
        {
            return Err(format_error(invalid(
                "DV removal requires a new replacement DV",
            )));
        }
        OperationIntent::new(OperationIntentParts {
            target: self.target.clone(),
            target_ref: self.target_ref.clone(),
            start: self.start,
            changes: FileChanges { added, removed },
            dependencies: self.dependencies.clone(),
            isolation: IsolationLevel::Serializable,
            shape: RequestShape::SnapshotProducing,
            summary: self.summary.clone(),
            token: operation.token(),
        })
    }

    fn preparer(&self) -> Result<Box<dyn Preparer>, ConnectorError> {
        // A managed publication admitted as CommitEmptyWrite still needs its
        // snapshot marker even when no row delta was materialized.
        if self.files.is_empty()
            && matches!(
                self.op_kind,
                CommitOpKind::RowDelta | CommitOpKind::RowDeltaDvFromFiles
            )
            && !self.summary.is_empty()
        {
            return Ok(Box::new(crate::commit::fast_append::FastAppendPreparer));
        }
        Ok(match self.op_kind {
            CommitOpKind::FastAppend => Box::new(crate::commit::fast_append::FastAppendPreparer),
            CommitOpKind::Overwrite => Box::new(crate::commit::overwrite::OverwritePreparer),
            CommitOpKind::RowDelta => Box::new(crate::commit::row_delta::RowDeltaPreparer),
            CommitOpKind::RowDeltaDvFromFiles => {
                Box::new(crate::commit::row_delta_dv_from_files::RowDeltaDvFromFilesPreparer)
            }
            CommitOpKind::CowUpdate => Box::new(crate::commit::update_cow::CowUpdatePreparer {
                rewrite: self
                    .cow
                    .clone()
                    .ok_or_else(|| invalid("COW graph is absent"))?,
            }),
            CommitOpKind::SelectedRewrite => {
                Box::new(crate::commit::selected_rewrite::SelectedRewritePreparer {
                    files: self
                        .selected
                        .clone()
                        .ok_or_else(|| invalid("Selected rewrite input is absent"))?,
                })
            }
            CommitOpKind::OverwritePartitions => {
                Box::new(crate::commit::overwrite_partitions::OverwritePartitionsPreparer)
            }
            CommitOpKind::Truncate => Box::new(crate::commit::truncate::TruncatePreparer),
        })
    }
}

fn cow_output_row_ids(
    rewrite: &CowUpdateRewriteSet,
) -> Result<BTreeMap<String, Option<i64>>, ConnectorError> {
    let mut markers = BTreeMap::new();
    for touched in &rewrite.touched_data_files {
        let min = touched
            .row_ids
            .iter()
            .copied()
            .min()
            .filter(|min| *min >= 0)
            .ok_or_else(|| corrupt("COW replacement has no valid source row IDs"))?;
        if touched.row_ids.iter().any(|id| *id < 0) {
            return Err(corrupt("COW source row ID is negative"));
        }
        for path in &touched.new_files {
            if markers.insert(path.clone(), Some(min)).is_some() {
                return Err(corrupt("Duplicate COW replacement output"));
            }
        }
    }
    for file in &rewrite.appended_files {
        if markers.insert(file.path.clone(), None).is_some() {
            return Err(corrupt("COW append overlaps another output"));
        }
    }
    Ok(markers)
}

struct WriteStatisticsPreparer {
    op_kind: CommitOpKind,
    drafts: Vec<novarocks_spi::connector::StatisticsArtifactDraft>,
}

#[async_trait]
impl Preparer for WriteStatisticsPreparer {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        _intent: &OperationIntent,
    ) -> crate::iceberg::Result<PreparedChange> {
        if self.drafts.is_empty() {
            return Ok(PreparedChange::default());
        }
        let metadata = view.metadata();
        let snapshot = metadata
            .snapshot_for_ref(view.target_ref())
            .ok_or_else(|| {
                format_error(corrupt("Statistics preparation has no staged snapshot"))
            })?;
        let drafts = artifacts_for_staged_metadata(
            metadata,
            view.artifacts().file_io(),
            snapshot.parent_snapshot_id(),
            metadata,
            view.target_ref(),
            self.op_kind,
            self.drafts.clone(),
        )
        .await;
        view.artifacts().check_active()?;
        let drafts = drafts.map_err(format_error)?;
        let statistics = crate::stats_assembler::write_puffin_artifacts_allocated(
            view.artifacts(),
            snapshot.snapshot_id(),
            snapshot.sequence_number(),
            &drafts,
        )
        .await;
        view.artifacts().check_active()?;
        let statistics = statistics.map_err(|e| format_error(corrupt(e)))?;
        Ok(PreparedChange {
            requirements: vec![],
            updates: statistics
                .map(|statistics| crate::iceberg::TableUpdate::SetStatistics { statistics })
                .into_iter()
                .collect(),
        })
    }
}

#[derive(Clone)]
enum DispatchFact {
    Undispatched,
    Ready(ExternalMutationEvidence),
    Issued(ExternalMutationEvidence),
    Committed(CommitProof, ExternalMutationFinalization),
    Rejected(ConnectorMutationFailure),
}

struct ObservedPublisher {
    inner: Box<dyn Publisher>,
    state: Arc<Mutex<DispatchFact>>,
}

#[async_trait]
impl Publisher for ObservedPublisher {
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
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let DispatchFact::Ready(evidence) = &*state else {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::Internal,
                    "Iceberg dispatch has no complete bounded recovery evidence",
                );
            };
            *state = DispatchFact::Issued(evidence.clone());
        }
        let outcome = self.inner.dispatch_once(request).await;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &outcome {
            CatalogOutcome::KnownCommitted {
                receipt,
                finalization,
                ..
            } => *state = DispatchFact::Committed(receipt.clone(), finalization.clone()),
            CatalogOutcome::KnownUncommitted { failure } => {
                *state = DispatchFact::Rejected(failure.clone())
            }
            CatalogOutcome::Unsupported(error) => {
                *state = DispatchFact::Rejected(failure(
                    ConnectorMutationFailureKind::Unsupported,
                    error.message(),
                ))
            }
            CatalogOutcome::CommitUnknown { .. } => {}
        }
        outcome
    }
}

impl IcebergWriteSessionControl {
    pub(super) fn dispatch_operation(
        &self,
        handle: &IcebergCommitHandle,
        validated: &[ValidatedFragment<'_>],
        statistics: Vec<novarocks_spi::connector::StatisticsArtifactDraft>,
        documents: Option<&novarocks_spi::connector::ConnectorDocumentPublicationIntent>,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let operation = IcebergCommitOperation::new(
            OperationToken::from_write_session(handle.session_id()),
            handle.table().table_location(),
            self.runtime.resources().planning_binding().clone(),
            context.clone(),
            self.runtime.resources().catalog_runtime().clone(),
            OperationLimits::default(),
        )
        .map_err(|e| internal(e.to_string()))?;
        // Physical ownership deduplicates objects, while intent keeps full DV identities.
        let objects: BTreeSet<_> = validated
            .iter()
            .map(|entry| entry.fragment.path().to_owned())
            .collect();
        for path in objects {
            operation
                .adopt_session_data(ObjectIdentity::new(path).map_err(|e| corrupt(e.to_string()))?)
                .map_err(|e| internal(e.to_string()))?;
        }
        let recipe = WriteRecipe::freeze(handle, validated, documents);
        let recipe = match recipe {
            Ok(recipe) => recipe,
            Err(error) => return Ok(self.reject_operation(&operation, error)),
        };
        let data = match recipe.preparer() {
            Ok(data) => data,
            Err(error) => return Ok(self.reject_operation(&operation, error)),
        };
        let policy = match RetryPolicy::from_properties(recipe.source.properties()) {
            Ok(policy) => policy,
            Err(error) => {
                return Ok(self.reject_operation(&operation, format_error_to_connector(error)));
            }
        };
        let state = Arc::new(Mutex::new(DispatchFact::Undispatched));
        let template =
            match RecoveryTemplate::from_handle(&self.descriptor, self.key.incarnation, handle) {
                Ok(template) => template,
                Err(error) => return Ok(self.reject_operation(&operation, error)),
            };
        let preflight_state = Arc::clone(&state);
        let retained_evidence = handle.recovery_evidence_slot();
        let preflight = Arc::new(
            move |request: &FrozenRequest, operation: &IcebergCommitOperation| {
                let evidence = template.encode(request, operation).map_err(format_error)?;
                *retained_evidence.lock().map_err(|_| {
                    format_error(internal("Iceberg retained evidence state poisoned"))
                })? = Some(evidence.clone());
                *preflight_state.lock().map_err(|_| {
                    format_error(internal("Iceberg dispatch evidence state poisoned"))
                })? = DispatchFact::Ready(evidence);
                Ok(())
            },
        );
        let catalog = Arc::clone(self.runtime.novarocks_catalog());
        let evidence = CatalogCommitEvidence::for_target(format!(
            "{}.{}",
            handle.table().namespace(),
            handle.table().table_name()
        ))
        .with_target_uuid(handle.table().table_uuid().to_string())
        .with_commit_uuid(handle.session_id().to_string());
        let direct = TransitionalPublisher {
            catalog,
            ident: recipe.target.ident.clone(),
            target_ref: recipe.target_ref.clone(),
            evidence,
            recovery_preflight: preflight,
        };
        let inner: Box<dyn Publisher> = if matches!(
            recipe.op_kind,
            CommitOpKind::FastAppend | CommitOpKind::Overwrite
        ) {
            Box::new(OwnerPublisher {
                target: direct,
                operation: operation.token(),
                marker: Some((
                    Arc::from(ICEBERG_WRITE_SESSION_MARKER_PROPERTY),
                    Arc::from(handle.session_id().to_string()),
                )),
            })
        } else {
            Box::new(direct)
        };
        let publisher = ObservedPublisher {
            inner,
            state: Arc::clone(&state),
        };
        let stats = WriteStatisticsPreparer {
            op_kind: recipe.op_kind,
            drafts: statistics,
        };
        let output_facts = recipe.output_facts.clone();
        let run_operation = operation.clone();
        let result = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                let intent = match recipe.intent(&run_operation).await {
                    Ok(intent) => intent,
                    Err(error) => {
                        return attempt::rejected(
                            &run_operation,
                            attempt::before_dispatch_failure(&error),
                        )
                        .await;
                    }
                };
                attempt::run(
                    &run_operation,
                    &intent,
                    &recipe.prefix,
                    &[data.as_ref(), &stats],
                    &publisher,
                    policy,
                )
                .await
            });
        let report = match result {
            Ok(report) => report,
            Err(error) => {
                let observed = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                match observed {
                    DispatchFact::Issued(evidence) => {
                        return Ok(ExternalMutationOutcome::CommitUnknown {
                            failure: failure(ConnectorMutationFailureKind::Unavailable, error),
                            evidence,
                        });
                    }
                    DispatchFact::Committed(proof, finalization) => {
                        *state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            DispatchFact::Committed(
                                proof.clone(),
                                add_finalization_failure(finalization, &error),
                            );
                        attempt::Report {
                            publication: PublicationOutcome::Committed(proof),
                            cleanup: IcebergCleanupReport::Partial {
                                deleted: 0,
                                remaining: operation
                                    .recovery_artifacts()
                                    .into_iter()
                                    .map(|r| {
                                        crate::commit::model::RemainingArtifact {
                                    object: r.object,
                                    reason:
                                        crate::commit::model::CleanupRemainingReason::DeleteFailed(
                                            error.clone(),
                                        ),
                                }
                                    })
                                    .collect(),
                            },
                        }
                    }
                    DispatchFact::Rejected(failure) => {
                        return Ok(self.reject_operation_failure(&operation, failure));
                    }
                    DispatchFact::Undispatched | DispatchFact::Ready(_) => {
                        return Ok(self.reject_operation(&operation, internal(error)));
                    }
                }
            }
        };
        match report.publication {
            PublicationOutcome::KnownUncommitted(failure) => {
                Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure,
                    cleanup: report.cleanup.finalization(),
                })
            }
            PublicationOutcome::Unknown { failure, .. } => {
                let observed = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match &*observed {
                    DispatchFact::Issued(evidence) => Ok(ExternalMutationOutcome::CommitUnknown {
                        failure,
                        evidence: evidence.clone(),
                    }),
                    _ => Err(internal(
                        "Unknown Iceberg dispatch lacks its checked recovery envelope",
                    )),
                }
            }
            PublicationOutcome::Committed(proof) => {
                let mut finalization = report.cleanup.finalization();
                if let DispatchFact::Committed(
                    _,
                    ExternalMutationFinalization::Failed(owner_failure),
                ) = &*state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                {
                    finalization = add_finalization_failure(finalization, owner_failure.message());
                }
                let rows = match proof.snapshot_id {
                    Some(id) => match self.publication_row_count(handle, id, context) {
                        Ok(rows) => rows,
                        Err(e) => {
                            finalization = add_finalization_failure(finalization, e.message());
                            None
                        }
                    },
                    None => None,
                };
                let (receipt, finalization) = match proof.snapshot_id {
                    Some(id) => committed_write_receipt(
                        id,
                        rows,
                        handle.repartition().map(|r| r.committed().clone()),
                        Some(output_facts),
                        finalization,
                    ),
                    None => (
                        unprojected_committed_receipt(),
                        add_finalization_failure(
                            finalization,
                            "Committed Iceberg write has no published snapshot projection",
                        ),
                    ),
                };
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: proof.effect,
                    receipt,
                    finalization,
                })
            }
        }
    }

    fn reject_operation(
        &self,
        operation: &IcebergCommitOperation,
        error: ConnectorError,
    ) -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        self.reject_operation_failure(
            operation,
            attempt::before_dispatch_failure(&format_error(error)),
        )
    }

    fn reject_operation_failure(
        &self,
        operation: &IcebergCommitOperation,
        failure: ConnectorMutationFailure,
    ) -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        let cleanup_operation = operation.clone();
        let cleanup = match self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                cleanup_operation
                    .cleanup(CleanupScope::EntireOperation)
                    .await
            }) {
            Ok(cleanup) => cleanup.finalization(),
            Err(error) => ExternalMutationFinalization::Failed(super::failure(
                ConnectorMutationFailureKind::Unavailable,
                format!("Iceberg pre-dispatch cleanup bridge failed: {error}"),
            )),
        };
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup }
    }
}

fn format_error_to_connector(error: crate::iceberg::Error) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveryPayload {
    pub version: u16,
    pub session_id: String,
    pub table_ident: String,
    pub table_uuid: String,
    pub target_ref: String,
    pub op_kind: String,
    pub base_snapshot_id: Option<i64>,
    pub base_sequence_number: i64,
    pub staging_dir: String,
    pub document_manifest_digest: Option<[u8; 32]>,
    operation_id: [u8; 16],
    attempt: RecoveryAttempt,
    parent_snapshot_id: Option<i64>,
    metadata_location: String,
    published_snapshot_id: Option<i64>,
    artifacts: Vec<RecoveryObject>,
    attempt_owned: Vec<u32>,
    operation_references: Vec<u32>,
    session_references: Vec<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryAttempt {
    ordinal: u32,
    nonce: [u8; 16],
}
impl From<crate::commit::model::AttemptToken> for RecoveryAttempt {
    fn from(token: crate::commit::model::AttemptToken) -> Self {
        Self {
            ordinal: token.ordinal(),
            nonce: token.nonce_bytes(),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OwnedClass {
    SessionData,
    Operation,
    Attempt,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WriteState {
    Allocated,
    Writing,
    Written,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryObject {
    path: String,
    class: OwnedClass,
    attempt: Option<RecoveryAttempt>,
    state: WriteState,
}

struct RecoveryTemplate {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    session_id: IcebergWriteSessionId,
    table_ident: String,
    table_uuid: String,
    target_ref: String,
    op_kind: String,
    base_snapshot_id: Option<i64>,
    base_sequence_number: i64,
    staging_dir: String,
    document_manifest_digest: Option<[u8; 32]>,
}

impl RecoveryTemplate {
    fn from_handle(
        descriptor: &ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        handle: &IcebergCommitHandle,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            descriptor: descriptor.clone(),
            incarnation,
            session_id: handle.session_id(),
            table_ident: format!(
                "{}.{}",
                handle.table().namespace(),
                handle.table().table_name()
            ),
            table_uuid: handle.table().table_uuid().to_string(),
            target_ref: handle.table().target_ref().to_string(),
            op_kind: format!("{:?}", handle.commit_op_kind()),
            base_snapshot_id: handle.table().base_snapshot_id(),
            base_sequence_number: handle.table().base_sequence_number(),
            staging_dir: handle.staging_dir(),
            document_manifest_digest: handle
                .document_manifest()?
                .as_deref()
                .map(crate::document_storage::publication::prepared_manifest_digest),
        })
    }

    fn encode(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        if operation.token() != OperationToken::from_write_session(self.session_id)
            || request.artifacts().attempt().operation() != operation.token()
        {
            return Err(internal(
                "Recovery envelope operation differs from its frozen request",
            ));
        }
        let BaseIdentity::Existing {
            uuid,
            parent,
            metadata_location,
        } = request.base()
        else {
            return Err(invalid(
                "Existing write recovery cannot encode a create request",
            ));
        };
        if uuid.to_string() != self.table_uuid
            || request.target_ref() != self.target_ref
            || request.shape() != RequestShape::SnapshotProducing
        {
            return Err(invalid(
                "Recovery envelope target differs from its frozen request",
            ));
        }
        let records = operation.artifacts().map_err(|e| internal(e.to_string()))?;
        let mut index = BTreeMap::new();
        let mut artifacts = Vec::new();
        for record in records {
            index.insert(
                record.object.clone(),
                u32::try_from(artifacts.len())
                    .map_err(|_| internal("Recovery object count overflow"))?,
            );
            let class = match record.class {
                ArtifactClass::SessionData => OwnedClass::SessionData,
                ArtifactClass::Operation => OwnedClass::Operation,
                ArtifactClass::Attempt => OwnedClass::Attempt,
                ArtifactClass::ExternalRegistered => {
                    return Err(internal(
                        "External registered object entered the owned ledger",
                    ));
                }
            };
            let state = match record.write_state {
                crate::commit::model::ArtifactWriteState::Allocated => WriteState::Allocated,
                crate::commit::model::ArtifactWriteState::Writing => WriteState::Writing,
                crate::commit::model::ArtifactWriteState::Written => WriteState::Written,
            };
            artifacts.push(RecoveryObject {
                path: record.object.path().to_string(),
                class,
                attempt: record.attempt.map(RecoveryAttempt::from),
                state,
            });
        }
        let indices = |paths: &[ObjectIdentity]| -> Result<Vec<u32>, ConnectorError> {
            paths
                .iter()
                .map(|object| {
                    index.get(object).copied().ok_or_else(|| {
                        internal("Frozen request references an object absent from recovery ledger")
                    })
                })
                .collect()
        };
        let payload = RecoveryPayload {
            version: ICEBERG_WRITE_SESSION_EVIDENCE_VERSION,
            session_id: self.session_id.to_string(),
            table_ident: self.table_ident.clone(),
            table_uuid: self.table_uuid.clone(),
            target_ref: self.target_ref.clone(),
            op_kind: self.op_kind.clone(),
            base_snapshot_id: self.base_snapshot_id,
            base_sequence_number: self.base_sequence_number,
            staging_dir: self.staging_dir.clone(),
            document_manifest_digest: self.document_manifest_digest,
            operation_id: operation.token().to_bytes(),
            attempt: request.artifacts().attempt().into(),
            parent_snapshot_id: *parent,
            metadata_location: metadata_location.clone(),
            published_snapshot_id: request.ref_snapshot_after(&self.target_ref),
            attempt_owned: indices(request.artifacts().attempt_owned())?,
            operation_references: indices(request.artifacts().operation_references())?,
            session_references: indices(request.artifacts().session_references())?,
            artifacts,
        };
        payload.validate_facts(self)?;
        let bytes = super::recovery_codec::encode(&payload)?;
        ExternalMutationEvidence::try_new(
            ICEBERG_WRITE_SESSION_EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.incarnation,
            ConnectorMutationOperationId::from_bytes(self.session_id.to_bytes()),
            ICEBERG_WRITE_SESSION_OPERATION_KIND,
            Bytes::from(bytes),
        )
    }
}

impl RecoveryPayload {
    pub(super) fn validate(
        &self,
        descriptor: &ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        handle: &IcebergCommitHandle,
    ) -> Result<(), ConnectorError> {
        self.validate_facts(&RecoveryTemplate::from_handle(
            descriptor,
            incarnation,
            handle,
        )?)
    }
    fn validate_facts(&self, expected: &RecoveryTemplate) -> Result<(), ConnectorError> {
        if self.version != ICEBERG_WRITE_SESSION_EVIDENCE_VERSION
            || self.session_id != expected.session_id.to_string()
            || self.operation_id != expected.session_id.to_bytes()
            || self.table_ident != expected.table_ident
            || self.table_uuid != expected.table_uuid
            || self.target_ref != expected.target_ref
            || self.op_kind != expected.op_kind
            || self.base_snapshot_id != expected.base_snapshot_id
            || self.base_sequence_number != expected.base_sequence_number
            || self.staging_dir != expected.staging_dir
            || self.document_manifest_digest != expected.document_manifest_digest
            || self.metadata_location.is_empty()
        {
            return Err(corrupt(
                "Iceberg recovery envelope disagrees with exact admitted session facts",
            ));
        }
        let mut attempts = BTreeMap::from([(self.attempt.ordinal, self.attempt.nonce)]);
        let valid_nonce = |nonce: &[u8; 16]| uuid::Uuid::from_bytes(*nonce).get_version_num() == 7;
        if !valid_nonce(&self.attempt.nonce) {
            return Err(corrupt("Iceberg recovery attempt has no UUIDv7 nonce"));
        }
        let mut previous: Option<&str> = None;
        for object in &self.artifacts {
            crate::commit::write_stack::domain::validate_location("recovery object", &object.path)?;
            if previous.is_some_and(|path| path >= object.path.as_str()) {
                return Err(corrupt(
                    "Iceberg recovery ledger is not strictly ordered by unique physical path",
                ));
            }
            previous = Some(&object.path);
            match (object.class, &object.attempt) {
                (OwnedClass::Attempt, Some(attempt))
                    if attempt.ordinal <= self.attempt.ordinal && valid_nonce(&attempt.nonce) =>
                {
                    if attempts
                        .insert(attempt.ordinal, attempt.nonce)
                        .is_some_and(|old| old != attempt.nonce)
                    {
                        return Err(corrupt(
                            "Iceberg recovery attempt ordinal has conflicting nonces",
                        ));
                    }
                }
                (OwnedClass::SessionData, None) if object.state == WriteState::Written => {}
                (OwnedClass::Operation, None) => {}
                _ => {
                    return Err(corrupt(
                        "Iceberg recovery object class disagrees with its attempt/write state",
                    ));
                }
            }
        }
        let mut referenced = BTreeSet::new();
        for (references, class) in [
            (&self.attempt_owned, OwnedClass::Attempt),
            (&self.operation_references, OwnedClass::Operation),
            (&self.session_references, OwnedClass::SessionData),
        ] {
            let mut previous = None;
            for index in references {
                let object = self
                    .artifacts
                    .get(*index as usize)
                    .ok_or_else(|| corrupt("Recovery reference is outside the complete ledger"))?;
                if previous.is_some_and(|prev| prev >= *index)
                    || !referenced.insert(*index)
                    || object.class != class
                    || object.state != WriteState::Written
                    || (class == OwnedClass::Attempt
                        && object.attempt.as_ref() != Some(&self.attempt))
                {
                    return Err(corrupt(
                        "Recovery reference is duplicate or disagrees with exact ownership",
                    ));
                }
                previous = Some(*index);
            }
        }
        let all_current: Vec<_> = self
            .artifacts
            .iter()
            .enumerate()
            .filter(|(_, object)| {
                object.class == OwnedClass::Attempt
                    && object.attempt.as_ref() == Some(&self.attempt)
            })
            .map(|(i, _)| i as u32)
            .collect();
        if self.attempt_owned != all_current {
            return Err(corrupt("Recovery request omits current attempt objects"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::commit::model::{ArtifactKind, Dependency};
    use crate::iceberg::spec::FormatVersion;
    use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId, ConnectorStopOwner};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    struct Fixture {
        directory: tempfile::TempDir,
        operation: IcebergCommitOperation,
        metadata: TableMetadata,
        template: RecoveryTemplate,
        _stop: ConnectorStopOwner,
    }
    impl Fixture {
        fn new() -> Self {
            let original = crate::commit::overwrite::preparer_tests::Fixture::new();
            let metadata = original.metadata(FormatVersion::V3);
            let directory = original.directory;
            let location = format!("file://{}", directory.path().display());
            let runtime = tokio::runtime::Handle::current();
            let binding = crate::access_binding::IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
                Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime.clone())),
            );
            let stop = ConnectorStopOwner::new();
            let session_id = IcebergWriteSessionId::from_bytes([17; 16]);
            let operation = IcebergCommitOperation::new(
                OperationToken::from_write_session(session_id),
                location.clone(),
                binding,
                ConnectorRequestContext::try_new(
                    Instant::now() + Duration::from_secs(60),
                    stop.view(),
                    64 * 1024,
                    1024 * 1024,
                )
                .unwrap(),
                crate::resources::IcebergCatalogRuntime::new(runtime),
                OperationLimits::default(),
            )
            .unwrap();
            let template = RecoveryTemplate {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: ConnectorInstanceId::parse("recovery-test").unwrap(),
                },
                incarnation: ProviderBindingEpoch::from_bytes([29; 16]),
                session_id,
                table_ident: "db.t".into(),
                table_uuid: metadata.uuid().to_string(),
                target_ref: "main".into(),
                op_kind: "FastAppend".into(),
                base_snapshot_id: None,
                base_sequence_number: 0,
                staging_dir: location,
                document_manifest_digest: None,
            };
            Self {
                directory,
                operation,
                metadata,
                template,
                _stop: stop,
            }
        }
        fn intent(&self) -> OperationIntent {
            self.intent_with_files(Vec::new())
        }
        fn intent_with_files(&self, added: Vec<AddedContent>) -> OperationIntent {
            OperationIntent::new(OperationIntentParts {
                target: TableTarget {
                    ident: crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
                    uuid: Some(self.metadata.uuid()),
                },
                target_ref: "main".into(),
                start: None,
                changes: FileChanges {
                    added,
                    removed: Vec::new(),
                },
                dependencies: vec![Dependency::NoReadDependency],
                isolation: IsolationLevel::Serializable,
                shape: RequestShape::SnapshotProducing,
                summary: BTreeMap::from([(
                    "novarocks.write.session.v1".into(),
                    self.template.session_id.to_string(),
                )]),
                token: self.operation.token(),
            })
            .unwrap()
        }
        fn adopt_entropy(&self) -> ObjectIdentity {
            let mut path = self.directory.path().to_path_buf();
            // Full independent path entropy remains material after compression.
            for _ in 0..3 {
                let segment: String = (0..3)
                    .map(|_| uuid::Uuid::new_v4().simple().to_string())
                    .collect();
                path.push(segment);
            }
            std::fs::create_dir_all(&path).unwrap();
            path.push(format!("{}.parquet", uuid::Uuid::new_v4().simple()));
            std::fs::write(&path, b"owned file").unwrap();
            let object = ObjectIdentity::new(format!("file://{}", path.display())).unwrap();
            self.operation.adopt_session_data(object.clone()).unwrap();
            object
        }
        fn adopt_random_file(&self) -> ObjectIdentity {
            let path = self.directory.path().join(format!(
                "novarocks-00000-{}.parquet",
                uuid::Uuid::new_v4().simple()
            ));
            std::fs::write(&path, b"owned file").unwrap();
            let object = ObjectIdentity::new(format!("file://{}", path.display())).unwrap();
            self.operation.adopt_session_data(object.clone()).unwrap();
            object
        }
        fn adopt(&self, ordinal: usize) -> ObjectIdentity {
            let path = self
                .directory
                .path()
                .join(format!("data-{ordinal:04}.parquet"));
            std::fs::write(&path, b"owned file").unwrap();
            let object = ObjectIdentity::new(format!("file://{}", path.display())).unwrap();
            self.operation.adopt_session_data(object.clone()).unwrap();
            object
        }
    }

    struct TestPublisher<'a> {
        fixture: &'a Fixture,
        dispatches: AtomicUsize,
        checked: Mutex<Option<ExternalMutationEvidence>>,
    }
    #[async_trait]
    impl Publisher for TestPublisher<'_> {
        async fn load_target(
            &self,
            _: &IcebergCommitAttempt,
        ) -> crate::iceberg::Result<StagingBase> {
            Ok(StagingBase::Existing {
                metadata: self.fixture.metadata.clone(),
                metadata_location: format!(
                    "file://{}/base.metadata.json",
                    self.fixture.directory.path().display()
                ),
            })
        }
        fn preflight_recovery(
            &self,
            request: &FrozenRequest,
            operation: &IcebergCommitOperation,
        ) -> crate::iceberg::Result<()> {
            *self.checked.lock().unwrap() = Some(
                self.fixture
                    .template
                    .encode(request, operation)
                    .map_err(format_error)?,
            );
            Ok(())
        }
        async fn dispatch_once(&self, _: FrozenRequest) -> CatalogOutcome<CommitProof> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            CatalogOutcome::unknown(
                "transport lost after issue",
                CatalogCommitEvidence::for_target("db.t"),
            )
        }
    }
    fn publisher(fixture: &Fixture) -> TestPublisher<'_> {
        TestPublisher {
            fixture,
            dispatches: AtomicUsize::new(0),
            checked: Mutex::new(None),
        }
    }

    #[tokio::test]
    async fn complete_owned_ledger_over_capacity_refuses_dispatch_and_cleans_actual_objects() {
        let fixture = Fixture::new();
        let objects: Vec<_> = (0..800).map(|_| fixture.adopt_entropy()).collect();
        let publisher = publisher(&fixture);
        let report = attempt::run(
            &fixture.operation,
            &fixture.intent(),
            &PreparedChange::default(),
            &[&crate::commit::fast_append::FastAppendPreparer],
            &publisher,
            RetryPolicy::from_properties(fixture.metadata.properties()).unwrap(),
        )
        .await;
        assert!(
            matches!(report.publication, PublicationOutcome::KnownUncommitted(_)),
            "{report:?}"
        );
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 0);
        assert!(publisher.checked.lock().unwrap().is_none());
        assert!(
            matches!(report.cleanup, IcebergCleanupReport::Complete { .. }),
            "{:?}",
            report.cleanup
        );
        assert!(fixture.operation.artifacts().unwrap().is_empty());
        assert!(
            objects
                .iter()
                .all(|o| !std::path::Path::new(o.path().strip_prefix("file://").unwrap()).exists())
        );
    }

    #[tokio::test]
    async fn large_complete_ledger_codec_preserves_all_classes_attempts_and_references() {
        use crate::commit::staging::StagingEngine;
        use crate::iceberg::spec::Struct;
        let fixture = Fixture::new();
        let sessions: Vec<_> = (0..1221).map(|_| fixture.adopt_random_file()).collect();
        let old_attempt = fixture.operation.begin_attempt().unwrap();
        let old_object = old_attempt
            .allocate(ArtifactClass::Attempt, ArtifactKind::Statistics)
            .unwrap();
        let operation_object = old_attempt
            .allocate(ArtifactClass::Operation, ArtifactKind::Metadata)
            .unwrap();
        let mut writer = old_attempt
            .file_io()
            .new_output(operation_object.path())
            .unwrap()
            .writer()
            .await
            .unwrap();
        writer
            .write(Bytes::from_static(b"operation-owned metadata"))
            .await
            .unwrap();
        writer.close().await.unwrap();
        let added = sessions
            .iter()
            .map(|object| {
                AddedContent::new_logical_data(
                    crate::commit::overwrite::preparer_tests::data(
                        object.path(),
                        1,
                        Struct::empty(),
                    ),
                    fixture.metadata.default_partition_spec_id(),
                )
                .unwrap()
            })
            .collect();
        let intent = fixture.intent_with_files(added);
        let current = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: fixture.metadata.clone(),
                metadata_location: format!(
                    "file://{}/base.metadata.json",
                    fixture.directory.path().display()
                ),
            },
            &intent,
            &current,
        )
        .unwrap();
        engine
            .stage(&crate::commit::fast_append::FastAppendPreparer)
            .await
            .unwrap();
        let mut references = sessions.clone();
        references.push(operation_object.clone());
        let request = engine.freeze(&references).unwrap();
        let evidence = fixture
            .template
            .encode(&request, &fixture.operation)
            .unwrap();
        let payload: RecoveryPayload =
            super::super::recovery_codec::decode(evidence.provider_payload()).unwrap();
        payload.validate_facts(&fixture.template).unwrap();
        let records = fixture.operation.artifacts().unwrap();
        assert_eq!(payload.artifacts.len(), records.len());
        for (object, record) in payload.artifacts.iter().zip(records) {
            assert_eq!(object.path, record.object.path());
            assert_eq!(object.attempt, record.attempt.map(RecoveryAttempt::from));
            let class = match record.class {
                ArtifactClass::SessionData => OwnedClass::SessionData,
                ArtifactClass::Operation => OwnedClass::Operation,
                ArtifactClass::Attempt => OwnedClass::Attempt,
                ArtifactClass::ExternalRegistered => {
                    panic!("borrowed object entered actual owned test ledger")
                }
            };
            let state = match record.write_state {
                crate::commit::model::ArtifactWriteState::Allocated => WriteState::Allocated,
                crate::commit::model::ArtifactWriteState::Writing => WriteState::Writing,
                crate::commit::model::ArtifactWriteState::Written => WriteState::Written,
            };
            assert_eq!(object.class, class);
            assert_eq!(object.state, state);
        }
        let paths = |indices: &[u32]| -> Vec<ObjectIdentity> {
            indices
                .iter()
                .map(|index| {
                    ObjectIdentity::new(payload.artifacts[*index as usize].path.clone()).unwrap()
                })
                .collect()
        };
        assert_eq!(
            paths(&payload.attempt_owned),
            request.artifacts().attempt_owned()
        );
        assert_eq!(
            paths(&payload.operation_references),
            request.artifacts().operation_references()
        );
        assert_eq!(
            paths(&payload.session_references),
            request.artifacts().session_references()
        );
        assert_eq!(payload.session_references.len(), 1221);
        assert_eq!(payload.operation_references.len(), 1);
        assert!(!payload.attempt_owned.is_empty());
        let old = payload
            .artifacts
            .iter()
            .find(|object| object.path == old_object.path())
            .unwrap();
        assert_eq!(old.state, WriteState::Allocated);
        assert_eq!(old.attempt, Some(old_attempt.attempt_token().into()));
        let raw = serde_json::to_vec(&payload).unwrap();
        assert!(raw.len() > novarocks_spi::connector::MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES);
        assert!(
            evidence.provider_payload().len()
                <= novarocks_spi::connector::MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES
        );
        assert_eq!(
            super::super::recovery_codec::encode(&payload).unwrap(),
            evidence.provider_payload().as_ref()
        );
        println!(
            "Recovery codec actual owned objects={} raw_json_bytes={} encoded_bytes={}",
            payload.artifacts.len(),
            raw.len(),
            evidence.provider_payload().len()
        );
        assert_eq!(
            fixture.operation.artifacts().unwrap().len(),
            payload.artifacts.len()
        );
        assert!(sessions.iter().all(|object| {
            std::path::Path::new(object.path().strip_prefix("file://").unwrap()).exists()
        }));
    }

    #[tokio::test]
    async fn unknown_envelope_preserves_old_attempt_and_complete_ownership_without_cleanup() {
        let fixture = Fixture::new();
        let session_object = fixture.adopt(0);
        let old_attempt = fixture.operation.begin_attempt().unwrap();
        let old_object = old_attempt
            .allocate(ArtifactClass::Attempt, ArtifactKind::Statistics)
            .unwrap();
        let operation_object = old_attempt
            .allocate(ArtifactClass::Operation, ArtifactKind::Metadata)
            .unwrap();
        // Leave one old allocated output in the ledger: failed cleanup/recovery
        // must retain its actual state, rather than claiming all objects written.
        let mut writer = old_attempt
            .file_io()
            .new_output(operation_object.path())
            .unwrap()
            .writer()
            .await
            .unwrap();
        writer
            .write(Bytes::from_static(b"operation metadata"))
            .await
            .unwrap();
        writer.close().await.unwrap();
        let publisher = publisher(&fixture);
        let report = attempt::run(
            &fixture.operation,
            &fixture.intent(),
            &PreparedChange::default(),
            &[&crate::commit::fast_append::FastAppendPreparer],
            &publisher,
            RetryPolicy::from_properties(fixture.metadata.properties()).unwrap(),
        )
        .await;
        assert!(matches!(
            report.publication,
            PublicationOutcome::Unknown { .. }
        ));
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 1);
        assert!(matches!(report.cleanup, IcebergCleanupReport::NotAttempted));
        let evidence = publisher.checked.lock().unwrap().clone().unwrap();
        let payload: RecoveryPayload =
            super::super::recovery_codec::decode(evidence.provider_payload()).unwrap();
        payload.validate_facts(&fixture.template).unwrap();
        assert_eq!(payload.operation_id, fixture.operation.token().to_bytes());
        assert_eq!(
            payload.attempt.ordinal,
            old_attempt.attempt_token().ordinal() + 1
        );
        assert_eq!(
            payload.artifacts.len(),
            fixture.operation.artifacts().unwrap().len()
        );
        let old = payload
            .artifacts
            .iter()
            .find(|a| a.path == old_object.path())
            .unwrap();
        assert_eq!(old.state, WriteState::Allocated);
        assert_eq!(old.attempt, Some(old_attempt.attempt_token().into()));
        assert!(
            payload
                .artifacts
                .iter()
                .any(|a| a.path == operation_object.path()
                    && a.class == OwnedClass::Operation
                    && a.state == WriteState::Written)
        );
        assert!(
            std::path::Path::new(session_object.path().strip_prefix("file://").unwrap()).exists()
        );
        let mut tampered = payload.clone();
        tampered.base_sequence_number += 1;
        assert!(tampered.validate_facts(&fixture.template).is_err());
        let mut tampered = payload.clone();
        tampered.attempt.nonce = [0; 16];
        assert!(tampered.validate_facts(&fixture.template).is_err());
        let mut tampered = payload.clone();
        tampered.attempt_owned.clear();
        assert!(tampered.validate_facts(&fixture.template).is_err());
        let mut json = serde_json::to_value(&payload).unwrap();
        json["deleted_objects"] = serde_json::json!([]);
        assert!(serde_json::from_value::<RecoveryPayload>(json).is_err());
        let mut nested = serde_json::to_value(&payload).unwrap();
        nested["artifacts"][0]["unknown"] = serde_json::json!(true);
        let nested = super::super::recovery_codec::encode(&nested).unwrap();
        assert!(super::super::recovery_codec::decode::<RecoveryPayload>(&nested).is_err());
    }
}
