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

//! Freeze once from admission; every catalog conflict prepares fresh metadata.

mod lineage;

use super::*;
#[cfg(test)]
use crate::catalog::error::CatalogCommitEvidence;
#[cfg(test)]
use crate::catalog::transaction::CommitProof;
#[cfg(test)]
use crate::commit::attempt::Publisher;
use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::attempt::prepared::CatalogPublisher;
use crate::commit::attempt::{self, RetryPolicy};
use crate::commit::dependency::ValidationInputs;
use crate::commit::model::{
    AddedContent, ArtifactClass, BaseIdentity, CleanupScope, FileChanges, FrozenRequest,
    IsolationLevel, ObjectIdentity, OperationIntent, OperationIntentParts, OperationToken,
    RequestShape, StartSnapshot, TableTarget,
};
#[cfg(test)]
use crate::commit::model::{ArtifactWriter, IcebergCleanupReport, PublicationOutcome};
#[cfg(test)]
use crate::commit::operation::IcebergCommitAttempt;
use crate::commit::operation::{IcebergCommitOperation, OperationLimits};
#[cfg(test)]
use crate::commit::staging::StagingBase;
use crate::commit::staging::{PreparedChange, Preparer, StagedView};
use async_trait::async_trait;
#[cfg(test)]
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
    keyed_documents: Option<novarocks_spi::connector::KeyedPublication>,
}

impl WriteRecipe {
    fn freeze(
        handle: &IcebergCommitHandle,
        validated: &[ValidatedFragment<'_>],
        documents: Option<&novarocks_spi::connector::KeyedPublicationIntent>,
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
        let mut summary = session_snapshot_properties_for_commit(handle)?;
        let keyed_documents = documents.map(|intent| intent.documents.clone());
        if let Some(keyed) = &keyed_documents {
            for record in &keyed.records {
                let key =
                    crate::document_storage::keyed::keys::record_key(&record.owner, &record.name);
                let value = crate::document_storage::keyed::record::encode_record(record)?;
                if summary.insert(key, value).is_some() {
                    return Err(invalid(
                        "keyed record conflicts with a provider summary field",
                    ));
                }
            }
        }
        let dependencies = vec![if matches!(
            handle.commit_op_kind(),
            CommitOpKind::FastAppend | CommitOpKind::Overwrite
        ) && handle.repartition().is_none()
            && handle.document_publication().is_none()
        {
            Dependency::NoRead
        } else {
            Dependency::RefUnchanged
        }];
        let mut prefix = if let Some(repartition) = handle.repartition() {
            repartition.prepared_change().clone()
        } else {
            PreparedChange::default()
        };
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
            keyed_documents,
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
        .map(|intent| {
            if self
                .summary
                .keys()
                .any(|key| key.starts_with(crate::document_storage::keyed::keys::RECORD_PREFIX))
            {
                intent.with_snapshot_loading(crate::catalog::SnapshotLoading::Refs)
            } else {
                intent
            }
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

struct KeyedPublicationPreparer(novarocks_spi::connector::KeyedPublication);
#[async_trait]
impl Preparer for KeyedPublicationPreparer {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> crate::iceberg::Result<PreparedChange> {
        // Recompute closure and budget on this attempt's actual refs base.
        // A concurrent metadata-only marker write may increase its byte size
        // without advancing main; the admitted source is not that base.
        let prepared =
            crate::document_storage::keyed::write::publication_updates(view.metadata(), &self.0)
                .map_err(format_error)?;
        for (key, value) in &prepared.summary {
            if intent.summary().get(key) != Some(value) {
                return Err(format_error(invalid(
                    "keyed publication record differs from its frozen intent",
                )));
            }
        }
        Ok(PreparedChange {
            requirements: vec![],
            updates: prepared.updates,
        })
    }
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

pub(super) enum WritePublicationVerdict {
    KnownCommitted {
        effect: ExternalMutationEffect,
        receipt: ConnectorWriteReceipt,
        finalization: ExternalMutationFinalization,
        cleanup: novarocks_spi::connector::write_stack::ConnectorWriteCleanupState,
    },
    KnownUncommitted {
        failure: ConnectorMutationFailure,
        cleanup: ExternalMutationFinalization,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}
impl WritePublicationVerdict {
    fn from_outcome(outcome: ExternalMutationOutcome<ConnectorWriteReceipt>) -> Self {
        match outcome {
            ExternalMutationOutcome::KnownCommitted {
                effect,
                receipt,
                finalization,
                cleanup,
            } => Self::KnownCommitted {
                effect,
                receipt,
                finalization,
                cleanup:
                    novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Observed(
                        cleanup,
                    ),
            },
            ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
                Self::KnownUncommitted { failure, cleanup }
            }
            ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
                Self::CommitUnknown { failure, evidence }
            }
        }
    }
    pub(super) fn into_completion(
        self,
    ) -> novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion {
        match self {
            Self::KnownCommitted { effect, receipt, finalization, cleanup } => {
                let mut canonical = novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::from_committed_frontier(effect, receipt, finalization);
                let novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { cleanup: original, .. } = &mut canonical else { unreachable!() };
                *original = cleanup;
                canonical
            },
            Self::KnownUncommitted { failure, cleanup } => novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownUncommitted { failure, cleanup },
            Self::CommitUnknown { failure, evidence } => novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::CommitUnknown { failure, evidence },
        }
    }
}
pub(super) struct WritePublicationDispatch {
    pub(super) outcome: WritePublicationVerdict,
    pub(super) cleanup: Option<(
        IcebergCommitOperation,
        PublicationJournal<ConnectorWriteReceipt>,
    )>,
}
impl WritePublicationDispatch {
    fn settled(outcome: ExternalMutationOutcome<ConnectorWriteReceipt>) -> Self {
        Self {
            outcome: WritePublicationVerdict::from_outcome(outcome),
            cleanup: None,
        }
    }
}
impl IcebergWriteSessionControl {
    pub(super) async fn dispatch_operation_async(
        &self,
        handle: &IcebergCommitHandle,
        validated: &[ValidatedFragment<'_>],
        statistics: Vec<novarocks_spi::connector::StatisticsArtifactDraft>,
        documents: Option<&novarocks_spi::connector::KeyedPublicationIntent>,
        context: &ConnectorRequestContext,
    ) -> Result<WritePublicationDispatch, ConnectorError> {
        let (operation, journal) = self.adopt_received_operation(handle, validated, context)?;
        let recipe = WriteRecipe::freeze(handle, validated, documents);
        let recipe = match recipe {
            Ok(recipe) => recipe,
            Err(error) => {
                let error = if documents.is_some() {
                    crate::document_storage::keyed::write::refusal_with_retained_bytes_async(
                        &self.runtime,
                        handle.table().namespace(),
                        handle.table().table_name(),
                        context,
                        error,
                    )
                    .await
                } else {
                    error
                };
                return Ok(WritePublicationDispatch::settled(
                    self.reject_operation(handle, &operation, error).await,
                ));
            }
        };
        let data = match recipe.preparer() {
            Ok(data) => data,
            Err(error) => {
                return Ok(WritePublicationDispatch::settled(
                    self.reject_operation(handle, &operation, error).await,
                ));
            }
        };
        let policy = match RetryPolicy::from_properties(recipe.source.properties()) {
            Ok(policy) => policy,
            Err(error) => {
                return Ok(WritePublicationDispatch::settled(
                    self.reject_operation(handle, &operation, format_error_to_connector(error))
                        .await,
                ));
            }
        };
        let template =
            match RecoveryTemplate::from_handle(&self.descriptor, self.key.incarnation, handle) {
                Ok(template) => template,
                Err(error) => {
                    return Ok(WritePublicationDispatch::settled(
                        self.reject_operation(handle, &operation, error).await,
                    ));
                }
            };
        let preflight_journal = journal.clone();
        let receipt_partitioning = handle.repartition().map(|r| r.committed().clone());
        let receipt_output_facts = recipe.output_facts;
        let retained_evidence = handle.recovery_evidence_slot();
        let preflight = Arc::new(
            move |request: &FrozenRequest, operation: &IcebergCommitOperation| {
                let evidence = template.encode(request, operation).map_err(format_error)?;
                *retained_evidence.lock().map_err(|_| {
                    format_error(internal("Iceberg retained evidence state poisoned"))
                })? = Some(evidence.clone());
                // Mandatory receipt facts are frozen before mutation becomes
                // possible. Catalog reload and row-count enrichment are optional.
                let receipt = match request.ref_snapshot_after(request.target_ref()) {
                    Some(snapshot) => crate::write_codec::connector_write_receipt_with_partitioning_and_output_facts(
                        snapshot, None, receipt_partitioning.clone(), Some(receipt_output_facts),
                    ).map_err(|error| format_error(corrupt(error)))?,
                    None => unprojected_committed_receipt(),
                };
                preflight_journal.record(evidence, receipt, request)
            },
        );
        let catalog = Arc::clone(self.runtime.novarocks_catalog());
        let publisher = journal.observe_prepared(Arc::new(CatalogPublisher {
            catalog_operation: handle.flavor().catalog_operation(),
            catalog,
            ident: recipe.target.ident.clone(),
            target_ref: recipe.target_ref.clone(),
            operation: operation.token(),
            recovery_preflight: preflight,
        }));
        let stats = WriteStatisticsPreparer {
            op_kind: recipe.op_kind,
            drafts: statistics,
        };
        let output_facts = recipe.output_facts;
        let run_operation = operation.clone();
        let report = async move {
            let intent = match run_operation
                .runtime()
                .observe_preparation_async(
                    recipe.intent(&run_operation),
                    attempt::before_dispatch_failure,
                )
                .await
            {
                Ok(intent) => intent,
                Err(error) => {
                    return attempt::rejected(
                        &run_operation,
                        attempt::before_dispatch_failure(&error),
                    )
                    .await;
                }
            };
            let keyed = recipe.keyed_documents.map(KeyedPublicationPreparer);
            let mut preparers: Vec<&dyn Preparer> = Vec::new();
            if let Some(keyed) = &keyed {
                preparers.push(keyed);
            }
            preparers.extend([data.as_ref(), &stats as &dyn Preparer]);
            attempt::run_to_committed_frontier(
                &run_operation,
                &intent,
                &recipe.prefix,
                &preparers,
                &publisher,
                policy,
            )
            .await
        }
        .await;
        // Before the first dispatch entry exists, the session itself retains
        // the complete original cleanup report. Nothing is encoded into a
        // second replay/terminal map, and no object list is reconstructed.
        let mut outcome = if journal.current_entry().is_none() {
            let cleanup = report.cleanup.finalization();
            handle.retain_early_cleanup(report.cleanup);
            match report.publication {
                crate::commit::model::PublicationOutcome::KnownUncommitted(failure) => {
                    WritePublicationVerdict::KnownUncommitted { failure, cleanup }
                }
                _ => unreachable!("publication without a dispatch entry is uncommitted"),
            }
        } else {
            match journal.project_retained_committed_frontier(report) {
                crate::commit::attempt::observation::RetainedPublicationProjection::Committed {
                    effect,
                    receipt,
                    finalization,
                } => WritePublicationVerdict::KnownCommitted {
                    effect,
                    receipt,
                    finalization,
                    cleanup:
                        novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Pending,
                },
                crate::commit::attempt::observation::RetainedPublicationProjection::Settled(
                    outcome,
                ) => WritePublicationVerdict::from_outcome(outcome),
            }
        };
        if let Some(evidence) = journal
            .current_entry()
            .and_then(|entry| journal.entry_evidence(entry))
            && let Err(error) = handle.retain_recovery_evidence(evidence)
        {
            // A local custody error cannot erase the journal's original
            // dispatch fact. Unknown retains its existing checked evidence.
            match &mut outcome {
                WritePublicationVerdict::KnownCommitted { finalization, .. } => {
                    *finalization = add_finalization_failure(finalization.clone(), error.message());
                }
                WritePublicationVerdict::KnownUncommitted { cleanup, .. } => {
                    *cleanup = add_finalization_failure(cleanup.clone(), error.message());
                }
                WritePublicationVerdict::CommitUnknown { .. } => {}
            }
        }
        let WritePublicationVerdict::KnownCommitted {
            cleanup,
            effect,
            receipt,
            mut finalization,
        } = outcome
        else {
            return Ok(WritePublicationDispatch {
                outcome,
                cleanup: None,
            });
        };
        let snapshot_id =
            journal
                .current_entry()
                .and_then(|entry| match journal.commit_outcome(entry) {
                    CatalogOutcome::KnownCommitted { receipt, .. } => receipt.snapshot_id,
                    _ => None,
                });
        let enriched = match snapshot_id {
            Some(snapshot_id) => {
                let rows = match receive_existing_finalization_context(
                    handle,
                    &journal,
                    journal
                        .current_entry()
                        .expect("committed write has a publication entry"),
                    context,
                    self.runtime.novarocks_catalog().evidence_policy(),
                ) {
                    Ok(finalization_context) => {
                        let _finalizing = journal
                            .current_entry()
                            .and_then(|entry| journal.finalization_activity(entry));
                        match self
                            .publication_row_count_async(handle, snapshot_id, &finalization_context)
                            .await
                        {
                            Ok(rows) => rows,
                            Err(error) => {
                                finalization =
                                    add_finalization_failure(finalization, error.message());
                                None
                            }
                        }
                    }
                    Err(error) => {
                        finalization = add_finalization_failure(finalization, error.message());
                        None
                    }
                };
                let (enriched, projection) = committed_write_receipt(
                    snapshot_id,
                    rows,
                    handle.repartition().map(|r| r.committed().clone()),
                    Some(output_facts),
                    finalization,
                );
                finalization = projection;
                enriched
            }
            None => receipt,
        };
        if let Some(entry) = journal.current_entry() {
            journal.record_entry_finalization(entry, finalization.clone());
        }
        Ok(WritePublicationDispatch {
            outcome: WritePublicationVerdict::KnownCommitted {
                cleanup,
                effect,
                receipt: enriched,
                finalization,
            },
            cleanup: Some((operation, journal)),
        })
    }

    fn adopt_received_operation(
        &self,
        handle: &IcebergCommitHandle,
        validated: &[ValidatedFragment<'_>],
        context: &ConnectorRequestContext,
    ) -> Result<
        (
            IcebergCommitOperation,
            PublicationJournal<ConnectorWriteReceipt>,
        ),
        ConnectorError,
    > {
        let operation = IcebergCommitOperation::new(
            OperationToken::from_write_session(handle.session_id()),
            handle.table().table_location(),
            self.runtime.resources().planning_binding().clone(),
            context.clone(),
            self.runtime.resources().catalog_runtime().clone(),
            OperationLimits::default(),
        )
        .map_err(|e| internal(e.to_string()))?;
        let journal = PublicationJournal::for_runtime(
            operation.token(),
            self.runtime.resources().catalog_runtime(),
        );
        handle.retain_publication(operation.clone(), journal.clone())?;
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
        Ok((operation, journal))
    }

    pub(super) async fn cleanup_received_set_async(
        &self,
        handle: &IcebergCommitHandle,
        received: &ConnectorPreparedWriteSet,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationFinalization, ConnectorError> {
        if !matches!(
            handle.state()?,
            IcebergWriteSessionState::KnownUncommitted { .. }
        ) {
            return Err(invalid(
                "received write cleanup requires a proven uncommitted session",
            ));
        }
        if handle.flavor() == IcebergWriteFlavor::StagedCreate {
            return Err(invalid(
                "staged target publication retains its own sealed object cleanup authority",
            ));
        }
        // The caller must have transferred the exact completed Session union;
        // the same Provider additionally checks binding/targets/fragment facts.
        // No ref/live-set scan and no old-reference path is adopted for deletion.
        let validated = validate_prepared_set(handle, &self.adapter, received)?;
        let (operation, _) = self.adopt_received_operation(handle, &validated, context)?;
        let cleanup = {
            let mut original = std::pin::pin!(operation.cleanup(CleanupScope::EntireOperation));
            let report =
                std::future::poll_fn(|cx| std::future::Future::poll(original.as_mut(), cx)).await;
            let cleanup = report.finalization();
            // The exact report enters the original handle before the actual
            // cleanup F drops at this scope's end, including Drop unwind.
            handle.retain_early_cleanup(report);
            cleanup
        };
        handle.retain_cleanup(cleanup.clone());
        handle.retain_finalization(cleanup.clone());
        Ok(cleanup)
    }

    async fn reject_operation(
        &self,
        handle: &IcebergCommitHandle,
        operation: &IcebergCommitOperation,
        error: ConnectorError,
    ) -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        self.reject_operation_failure(
            handle,
            operation,
            attempt::before_dispatch_failure(&format_error(error)),
        )
        .await
    }

    async fn reject_operation_failure(
        &self,
        handle: &IcebergCommitHandle,
        operation: &IcebergCommitOperation,
        failure: ConnectorMutationFailure,
    ) -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        let report = operation.cleanup(CleanupScope::EntireOperation).await;
        let cleanup = report.finalization();
        handle.retain_early_cleanup(report);
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
    pub document_record_digest: Option<[u8; 32]>,
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
    document_record_digest: Option<[u8; 32]>,
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
            document_record_digest: handle.document_records_digest()?,
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
            document_record_digest: self.document_record_digest,
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
            || self.document_record_digest != expected.document_record_digest
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
                document_record_digest: None,
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
                dependencies: vec![Dependency::NoRead],
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

#[cfg(test)]
mod keyed_preparation_tests {
    use super::*;
    use crate::commit::overwrite::preparer_tests::Fixture;
    use crate::iceberg::TableUpdate;
    use crate::iceberg::spec::FormatVersion;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorDocumentFormat, ConnectorDocumentVersion, ConnectorSnapshotRecord,
        KeyedPublication,
    };
    use std::collections::HashMap;

    fn document(name: &str, bytes: usize) -> ConnectorDocumentVersion {
        ConnectorDocumentVersion::try_new(
            "novarocks.test",
            name,
            ConnectorDocumentFormat::try_new("novarocks.test", "fact", 1).unwrap(),
            vec![],
            Bytes::from(vec![b'x'; bytes]),
        )
        .unwrap()
    }
    fn publication(version: ConnectorDocumentVersion) -> KeyedPublication {
        KeyedPublication {
            records: vec![ConnectorSnapshotRecord {
                owner: "novarocks.test".into(),
                name: "publication".into(),
                binding: Some(version.identity()),
                payload: Bytes::from_static(b"result"),
            }],
            roots: BTreeMap::from([("novarocks.test".into(), version.identity())]),
            versions: vec![version],
            ..Default::default()
        }
    }
    fn intent(
        fixture: &Fixture,
        metadata: &TableMetadata,
        documents: &KeyedPublication,
    ) -> OperationIntent {
        OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
                uuid: Some(metadata.uuid()),
            },
            target_ref: "main".into(),
            start: None,
            changes: FileChanges::default(),
            dependencies: vec![crate::commit::model::Dependency::RefUnchanged],
            isolation: IsolationLevel::Snapshot,
            shape: RequestShape::SnapshotProducing,
            summary: documents
                .records
                .iter()
                .map(|record| {
                    (
                        crate::document_storage::keyed::keys::record_key(
                            &record.owner,
                            &record.name,
                        ),
                        crate::document_storage::keyed::record::encode_record(record).unwrap(),
                    )
                })
                .collect(),
            token: fixture.operation.token(),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn keyed_publication_uses_actual_attempt_properties_before_writing_any_manifest() {
        use crate::commit::staging::StagingEngine;
        let fixture = Fixture::new();
        let mut base = fixture.metadata(FormatVersion::V2);
        let properties: HashMap<_, _> = (0..8)
            .map(|index| {
                let version = document(&format!("history-{index}"), 30_000);
                (
                    crate::document_storage::keyed::keys::version_key(version.identity()),
                    crate::document_storage::keyed::envelope::encode_version(&version).unwrap(),
                )
            })
            .collect();
        base = base
            .into_builder(None)
            .set_properties(properties)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let documents = publication(document("new-binding", 1));
        let intent = intent(&fixture, &base, &documents);
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: base,
                metadata_location: "file://base.metadata.json".into(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        let error = engine
            .stage(&KeyedPublicationPreparer(documents))
            .await
            .unwrap_err();
        assert_eq!(
            attempt::before_dispatch_failure(&error).kind(),
            ConnectorMutationFailureKind::InvalidRequest
        );
        assert!(error.to_string().contains("private properties"));
        assert!(fixture.operation.artifacts().unwrap().is_empty());
    }

    #[tokio::test]
    async fn keyed_publication_stages_records_directly_and_reasserts_the_current_binding() {
        use crate::commit::staging::StagingEngine;
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V2);
        let documents = publication(document("binding", 1));
        let intent = intent(&fixture, &base, &documents);
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: base,
                metadata_location: "file://base.metadata.json".into(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        engine
            .stage(&KeyedPublicationPreparer(documents.clone()))
            .await
            .unwrap();
        engine
            .stage(&crate::commit::fast_append::FastAppendPreparer)
            .await
            .unwrap();
        let snapshot = engine.metadata().current_snapshot().unwrap();
        assert_eq!(
            snapshot.summary().operation,
            crate::iceberg::spec::Operation::Append
        );
        assert_eq!(
            snapshot.summary().additional_properties["novarocks.doc.v2.record.novarocks.test~publication"],
            format!("2.{}.cmVzdWx0", documents.versions[0].identity().to_hex())
        );
        assert!(
            !snapshot
                .summary()
                .additional_properties
                .keys()
                .any(|key| crate::document_storage::keyed::keys::is_legacy_key(key))
        );
        let frozen = engine.freeze(&[]).unwrap();
        assert_eq!(frozen.artifacts().attempt_owned().len(), 1);
        let writes: Vec<_> = frozen
            .updates()
            .iter()
            .filter_map(|update| match update {
                TableUpdate::SetProperties { updates } => Some(updates),
                _ => None,
            })
            .collect();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].len(), 2); // one immutable version and this owner's root
    }
}
