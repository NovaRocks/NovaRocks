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

#[cfg(test)]
mod lineage;
mod managed_repartition;
pub(super) use managed_repartition::slot_matches;
mod materialized;
#[cfg(test)]
pub(crate) use materialized::observed_materialized_staging_fixture;
#[cfg(test)]
pub(in crate::commit::write_stack) use materialized::{
    MaterializedDeleteStep, observation_target_bytes,
};

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
    AddedContent, BaseIdentity, CleanupScope, CommitProtocol, FileChanges, FrozenRequest,
    ObjectIdentity, OperationIntent, OperationIntentParts, OperationToken, RequestShape,
    StartSnapshot, TableTarget, WriteSessionCommitProtocol, WriterSchemaSpec,
};
#[cfg(test)]
use crate::commit::model::{
    ArtifactOrigin, ArtifactWriter, IcebergCleanupReport, PublicationOutcome, ReceivedPublication,
};
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

/// Project exactly the admitted writer facts using the frozen write-time
/// metadata. Compact and whole-set publication share this author.
pub(in crate::commit) fn written_file_with_historical_partition(
    fragment: &IcebergCommitFragment,
    metadata: &TableMetadata,
    historical_partition: Option<&crate::iceberg::spec::StructType>,
) -> Result<WrittenFile, ConnectorError> {
    let partition = fragment
        .partition()
        .ok_or_else(|| corrupt("Iceberg result has no written partition"))?;
    let partition_values = match historical_partition {
        Some(ty) => crate::write_descriptor::decode_partition_descriptor_with_type(
            partition.descriptor(),
            ty,
        )
        .map_err(|error| corrupt(error.detail_message()))?,
        None => decode_partition_values(partition, metadata)?,
    };
    let metrics = fragment
        .metrics()
        .ok_or_else(|| corrupt("Iceberg result has no written metrics"))?;
    let record_count = metrics.record_count();
    let file_size_in_bytes = metrics.file_size_in_bytes();
    let empty_stats = crate::commit::report::IcebergColumnStats::default();
    let stats = metrics.column_stats().unwrap_or(&empty_stats);
    let (
        format,
        content,
        referenced_data_file,
        first_row_id,
        content_offset,
        content_size,
        cardinality,
        equality_ids,
    ) = match fragment.artifact() {
        IcebergCommitArtifact::DataFile(file) => (
            DataFileFormat::Parquet,
            DataContentType::Data,
            None,
            file.first_row_id(),
            None,
            None,
            None,
            None,
        ),
        IcebergCommitArtifact::PositionDeleteFile(file) => (
            DataFileFormat::Parquet,
            DataContentType::PositionDeletes,
            Some(file.referenced_data_file().to_string()),
            None,
            None,
            None,
            None,
            None,
        ),
        IcebergCommitArtifact::DeletionVector(file) => (
            DataFileFormat::Puffin,
            DataContentType::PositionDeletes,
            Some(file.referenced_data_file().to_string()),
            None,
            Some(file.content_range().offset()),
            Some(file.content_range().size_in_bytes()),
            Some(file.cardinality()),
            None,
        ),
        // An equality delete names no data file: Iceberg matches it against
        // every row whose equality-column values agree, so the manifest carries
        // the field ids instead of a referenced path.
        IcebergCommitArtifact::EqualityDeleteFile(file) => (
            DataFileFormat::Parquet,
            DataContentType::EqualityDeletes,
            None,
            None,
            None,
            None,
            None,
            Some(file.equality_field_ids().to_vec()),
        ),
        IcebergCommitArtifact::FinalDelete(effect) => {
            let crate::commit::write_stack::domain::IcebergFinalDeleteKind::ReplacePositions(file) =
                effect.kind()
            else {
                return Err(invalid("Iceberg pure removal has no written artifact"));
            };
            (
                DataFileFormat::Puffin,
                DataContentType::PositionDeletes,
                Some(file.referenced_data_file().to_owned()),
                None,
                Some(file.content_range().offset()),
                Some(file.content_range().size_in_bytes()),
                Some(file.cardinality()),
                None,
            )
        }
    };
    Ok(WrittenFile {
        path: fragment.path().to_string(),
        format,
        content,
        partition_values,
        partition_spec_id: partition.partition_spec_id(),
        record_count,
        file_size_in_bytes,
        split_offsets: metrics.split_offsets().to_vec(),
        column_sizes: unsigned_counts(&stats.column_sizes)?,
        value_counts: unsigned_counts(&stats.value_counts)?,
        null_value_counts: unsigned_counts(&stats.null_value_counts)?,
        nan_value_counts: unsigned_counts(&stats.nan_value_counts)?,
        lower_bounds: decode_bounds(&stats.lower_bounds, metadata)?,
        upper_bounds: decode_bounds(&stats.upper_bounds, metadata)?,
        key_metadata: None,
        referenced_data_file,
        equality_ids,
        first_row_id,
        content_offset,
        content_size_in_bytes: content_size,
        cardinality,
    })
}

pub(in crate::commit) struct RewriteFileBacking {
    pub(in crate::commit) file: usize,
    pub(in crate::commit) partition: usize,
}

/// Quote the file backing before copying any decoded fragment member. The
/// same quote covers the SDK's mutable builder while it clones the final file.
pub(in crate::commit) fn rewrite_file_backing(
    fragment: &IcebergCommitFragment,
    metadata: &TableMetadata,
) -> Result<RewriteFileBacking, ConnectorError> {
    use std::alloc::Layout;

    use crate::iceberg::spec::{Datum, Literal, PrimitiveType};
    use novarocks_type_contract::owned_resources::hashmap::fresh_table_layout;

    fn add(total: &mut usize, extra: usize) -> Result<(), ConnectorError> {
        *total = total.checked_add(extra).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg rewrite file backing extent is unrepresentable",
            )
        })?;
        Ok(())
    }

    fn array<T>(count: usize) -> Result<usize, ConnectorError> {
        Layout::array::<T>(count)
            .map(|layout| layout.size())
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::ResourceExhausted, error.to_string())
            })
    }

    fn map<T>(count: usize) -> Result<usize, ConnectorError> {
        fresh_table_layout::<i32, T>(count)
            .map(|facts| facts.request_bytes_upper_bound)
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::ResourceExhausted, error.to_string())
            })
    }

    fn literal_bytes(ty: &PrimitiveType, raw: &[u8]) -> usize {
        match ty {
            PrimitiveType::String | PrimitiveType::Fixed(_) | PrimitiveType::Binary => raw.len(),
            _ => 0,
        }
    }

    let IcebergCommitArtifact::DataFile(file) = fragment.artifact() else {
        return Err(corrupt("Iceberg rewrite received a non-data addition"));
    };
    let source_partition = file.partition();
    if source_partition.partition_spec_id() != metadata.default_partition_spec_id() {
        return Err(corrupt(
            "Iceberg rewrite output does not match its frozen partition spec",
        ));
    }
    let values = &source_partition.descriptor().values;
    let partition_type = metadata.default_partition_type();
    if values.len() != partition_type.fields().len() {
        return Err(corrupt(
            "Iceberg rewrite partition width differs from its frozen spec",
        ));
    }
    let mut partition = array::<Option<Literal>>(values.len())?;
    for (value, field) in values.iter().zip(partition_type.fields()) {
        if value.is_null {
            continue;
        }
        let bytes = value
            .datum_bytes
            .as_deref()
            .ok_or_else(|| corrupt("Iceberg rewrite partition is missing its encoded value"))?;
        let ty = field
            .field_type
            .as_primitive_type()
            .ok_or_else(|| corrupt("Iceberg rewrite partition field type is not primitive"))?;
        add(&mut partition, literal_bytes(ty, bytes))?;
    }

    let mut file_bytes = fragment.path().len();
    add(&mut file_bytes, partition)?;
    let metrics = file.metrics();
    add(
        &mut file_bytes,
        array::<i64>(metrics.split_offsets().len())?,
    )?;
    if let Some(stats) = metrics.column_stats() {
        for counts in [
            &stats.column_sizes,
            &stats.value_counts,
            &stats.null_value_counts,
            &stats.nan_value_counts,
        ] {
            add(&mut file_bytes, map::<u64>(counts.len())?)?;
        }
        let schema = metadata.current_schema();
        for bounds in [&stats.lower_bounds, &stats.upper_bounds] {
            add(
                &mut file_bytes,
                map::<Datum>(projected_bounds(bounds).count())?,
            )?;
            for (&field_id, raw) in projected_bounds(bounds) {
                add(
                    &mut file_bytes,
                    literal_bytes(bound_type(schema, field_id)?, raw),
                )?;
            }
        }
    }
    Ok(RewriteFileBacking {
        file: file_bytes,
        partition,
    })
}

struct WriteRecipe<'a> {
    source: Arc<TableMetadata>,
    files: Vec<WrittenFile>,
    compact_stage: Option<usize>,
    materialized: Option<Arc<materialized::MaterializedDeleteStep>>,
    fixed_mutation: bool,
    session_protocol: WriteSessionCommitProtocol,
    writer: WriterSchemaSpec,
    rewrite_removals: Option<Arc<[crate::commit::model::FrozenEntry]>>,
    start: Option<StartSnapshot>,
    target: TableTarget,
    target_ref: String,
    dependencies: Vec<crate::commit::model::Dependency>,
    op_kind: CommitOpKind,
    summary: BTreeMap<String, String>,
    prefix: PreparedChange,
    output_facts: crate::write_codec::IcebergWrittenOutputFacts,
    keyed_documents: Option<novarocks_spi::connector::KeyedPublication>,
    managed_overwrite: Option<managed_repartition::ManagedOverwrite<'a>>,
}

impl<'a> WriteRecipe<'a> {
    fn freeze(
        handle: &'a IcebergWriteCommitSession,
        validated: &[ValidatedFragment<'_>],
        documents: Option<&novarocks_spi::connector::KeyedPublicationIntent>,
    ) -> Result<Self, ConnectorError> {
        use crate::commit::model::Dependency;
        let facts = handle.table();
        let source = Arc::clone(
            handle
                .source_metadata_ref()
                .ok_or_else(|| internal("Iceberg write lost its admitted source metadata"))?,
        );
        let writer_metadata = handle
            .repartition()
            .map_or(source.as_ref(), |r| r.prospective_metadata());
        let fixed_mutation = handle.mutation_read().is_some();
        let historical = handle
            .mutation_read()
            .map(|recipe| {
                recipe.validate_loaded(facts, &source)?;
                let snapshot = facts
                    .base_snapshot_id()
                    .map(|id| {
                        source
                            .snapshot_by_id(id)
                            .ok_or_else(|| corrupt("Iceberg fixed write snapshot is absent"))
                    })
                    .transpose()?;
                let schema = match snapshot {
                    Some(snapshot) => snapshot
                        .schema(&source)
                        .map_err(|error| corrupt(error.to_string()))?,
                    None => source.current_schema().clone(),
                };
                crate::read_snapshot::frozen_partition_types(&source, &schema).map_err(corrupt)
            })
            .transpose()?;
        let mut targets = BTreeMap::new();
        let mut files = Vec::new();
        for entry in validated {
            let fragment = entry.fragment;
            let target = match fragment.artifact() {
                IcebergCommitArtifact::FinalDelete(effect) => Some(materialized::Target {
                    partition: effect.partition().clone(),
                    final_effect: Some(effect.clone()),
                }),
                IcebergCommitArtifact::PositionDeleteFile(file) if fixed_mutation => {
                    Some(materialized::Target {
                        partition: file.partition().clone(),
                        final_effect: None,
                    })
                }
                _ => None,
            };
            let historical_type = target
                .as_ref()
                .map(|target| {
                    historical
                        .as_ref()
                        .and_then(|types| types.get(&target.partition.partition_spec_id()))
                        .ok_or_else(|| {
                            corrupt("Iceberg materialized target has no historical partition type")
                        })
                })
                .transpose()?;
            if fragment.new_object_path().is_some() {
                let file = written_file_with_historical_partition(
                    fragment,
                    writer_metadata,
                    historical_type,
                )?;

                files.push(file);
            }
            if let Some(target) = target {
                let path = fragment
                    .referenced_data_file()
                    .ok_or_else(|| corrupt("Iceberg materialized effect has no target"))?;
                match targets.entry(path.to_owned()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(target);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if entry.get().final_effect.is_none()
                            && target.final_effect.is_none()
                            && entry.get().partition == target.partition => {}
                    std::collections::btree_map::Entry::Occupied(_) => {
                        return Err(corrupt(
                            "Iceberg materialized step has conflicting effects for one data file",
                        ));
                    }
                }
            }
        }
        let materialized = (!targets.is_empty())
            .then(|| Arc::new(materialized::MaterializedDeleteStep::new(targets)));
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
        let writer = WriterSchemaSpec {
            schema_id: facts.schema_id(),
            default_spec_id: facts.default_partition_spec_id(),
        };
        // A legacy operation keeps its original stage assertions.
        if let WriteSessionCommitProtocol::Legacy = handle.commit_protocol() {
            prefix.requirements.extend(writer.requirements());
        }
        let target = TableTarget {
            ident: crate::iceberg::TableIdent::from_strs([facts.namespace(), facts.table_name()])
                .map_err(|e| invalid(e.to_string()))?,
            uuid: Some(source.uuid()),
        };
        let managed_overwrite = managed_repartition::Effect::freeze(handle, &source)?;
        Ok(Self {
            source,
            files,
            compact_stage: None,
            materialized,
            fixed_mutation,
            session_protocol: handle.commit_protocol(),
            writer,
            rewrite_removals: handle.frozen_rewrite_removals().cloned(),
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
            managed_overwrite,
        })
    }

    async fn intent(
        &self,
        operation: &IcebergCommitOperation,
    ) -> crate::iceberg::Result<OperationIntent> {
        let mut added = Vec::new();
        for file in &self.files {
            let data = crate::commit::data_file::from_written_file(file)?;
            added.push(AddedContent::new_logical_data(
                data,
                file.partition_spec_id,
            )?);
        }
        let mut removed = Vec::new();
        if let Some(step) = &self.materialized {
            let snapshot_id = self
                .start
                .ok_or_else(|| format_error(invalid("materialized write has no source snapshot")))?
                .snapshot_id;
            let source_io = operation.begin_attempt()?;
            removed = step
                .observe(
                    Arc::clone(&self.source),
                    snapshot_id,
                    &source_io,
                    operation.request_context(),
                )
                .await?;
        } else if !self.fixed_mutation && self.op_kind == CommitOpKind::Truncate {
            let source_io = operation.begin_attempt()?;
            let inputs =
                ValidationInputs::new(&self.source, self.start.map(|s| s.snapshot_id), &source_io);
            removed.extend(
                inputs
                    .compact_live_entries()
                    .await?
                    .into_values()
                    .map(|entry| entry.frozen),
            );
        }
        let (dependencies, protocol) = match self.session_protocol {
            WriteSessionCommitProtocol::Legacy => {
                (self.dependencies.clone(), CommitProtocol::Legacy)
            }
            WriteSessionCommitProtocol::Append => (
                Vec::new(),
                CommitProtocol::Append {
                    output_spec_id: self.writer.default_spec_id,
                },
            ),
            WriteSessionCommitProtocol::RewriteFiles => (
                Vec::new(),
                CommitProtocol::RewriteFiles {
                    output_spec_id: self.writer.default_spec_id,
                    replaced: Arc::clone(self.rewrite_removals.as_ref().ok_or_else(|| {
                        format_error(internal("Iceberg rewrite lost its original removal facts"))
                    })?),
                },
            ),
            WriteSessionCommitProtocol::RowDelta(_) => {
                let window = operation.row_delta_check().ok_or_else(|| {
                    format_error(internal(
                        "Iceberg row-delta operation lost its frozen validation",
                    ))
                })?;
                (
                    Vec::new(),
                    CommitProtocol::RowDelta {
                        window: window.clone(),
                        writer: self.writer,
                    },
                )
            }
        };
        OperationIntent::new(OperationIntentParts {
            target: self.target.clone(),
            target_ref: self.target_ref.clone(),
            start: self.start,
            changes: FileChanges { added, removed },
            dependencies,
            protocol,
            shape: RequestShape::SnapshotProducing,
            summary: self.summary.clone(),
            token: operation.token(),
        })
        .and_then(|intent| {
            let intent = match self.compact_stage {
                Some(stage) => intent.with_compact_stage(stage)?,
                None => intent,
            };
            Ok(
                if matches!(self.session_protocol, WriteSessionCommitProtocol::Legacy)
                    && self.summary.keys().any(|key| {
                        key.starts_with(crate::document_storage::keyed::keys::RECORD_PREFIX)
                    })
                {
                    intent.with_snapshot_loading(crate::catalog::SnapshotLoading::Refs)
                } else {
                    intent
                },
            )
        })
    }

    fn preparer(
        &self,
        operation: &IcebergCommitOperation,
    ) -> Result<Box<dyn Preparer>, ConnectorError> {
        if let Some(step) = &self.materialized {
            return Ok(Box::new(Arc::clone(step)));
        }
        if self.session_protocol.operation() == crate::commit::model::CommitOperation::Optimize {
            let stage = self
                .compact_stage
                .ok_or_else(|| internal("Iceberg rewrite lost its original compact additions"))?;
            return Ok(Box::new(
                crate::commit::selected_rewrite::CompactRewritePreparer {
                    operation: operation.clone(),
                    stage,
                },
            ));
        }
        if self.compact_stage.is_some() {
            return Ok(Box::new(
                crate::commit::fast_append::CompactFastAppendPreparer(operation.clone()),
            ));
        }
        // A managed publication admitted as CommitEmptyWrite still needs its
        // snapshot marker even when no row delta was materialized.
        if self.files.is_empty()
            && matches!(
                self.op_kind,
                CommitOpKind::RowDelta | CommitOpKind::MaterializedRowDelta
            )
            && !self.summary.is_empty()
        {
            return Ok(Box::new(crate::commit::fast_append::FastAppendPreparer));
        }
        Ok(match self.op_kind {
            CommitOpKind::FastAppend => Box::new(crate::commit::fast_append::FastAppendPreparer),
            CommitOpKind::Overwrite => Box::new(crate::commit::overwrite::OverwritePreparer),
            CommitOpKind::RowDelta => Box::new(crate::commit::row_delta::RowDeltaPreparer),
            CommitOpKind::MaterializedRowDelta => {
                if !self.fixed_mutation {
                    return Err(corrupt(
                        "Iceberg v3 row delta lacks its complete materialization",
                    ));
                }
                Box::new(crate::commit::fast_append::FastAppendPreparer)
            }
            CommitOpKind::Truncate => Box::new(crate::commit::truncate::TruncatePreparer),
            CommitOpKind::SelectedRewrite => {
                return Err(internal("Iceberg rewrite lost its RewriteFiles protocol"));
            }
        })
    }
}

impl crate::commit::dependency::RemovedDataFiles for materialized::MaterializedDeleteStep {
    fn removed_data_file(
        &self,
        path: &str,
    ) -> crate::iceberg::Result<crate::commit::conflict::CandidateFile<'_>> {
        let (metadata, spec_id, file) = self.observed_data_file(path)?;
        Ok(crate::commit::conflict::CandidateFile {
            metadata,
            spec_id,
            file,
        })
    }
}

struct KeyedPublicationPreparer(novarocks_spi::connector::KeyedPublication);
#[async_trait]
impl Preparer for KeyedPublicationPreparer {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> crate::iceberg::Result<PreparedChange> {
        // Recompute closure on this attempt's actual refs base.
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
pub(super) struct ReceivedOperation<'a> {
    operation: IcebergCommitOperation,
    journal: PublicationJournal<ConnectorWriteReceipt>,
    recipe: Result<WriteRecipe<'a>, ConnectorError>,
}

impl IcebergWriteSessionControl {
    pub(super) fn prepare_received_operation<'a>(
        &self,
        handle: &'a IcebergWriteCommitSession,
        validated: &[ValidatedFragment<'_>],
        documents: Option<&novarocks_spi::connector::KeyedPublicationIntent>,
        context: &ConnectorRequestContext,
    ) -> Result<ReceivedOperation<'a>, ConnectorError> {
        let (operation, journal) = self.adopt_received_operation(handle, validated, context)?;
        let recipe = WriteRecipe::freeze(handle, validated, documents);
        Ok(ReceivedOperation {
            operation,
            journal,
            recipe,
        })
    }

    pub(super) fn prepare_compact_operation<'a>(
        &self,
        handle: &'a IcebergWriteCommitSession,
        received: &mut IcebergReceiving,
        documents: Option<&novarocks_spi::connector::KeyedPublicationIntent>,
        context: &ConnectorRequestContext,
    ) -> Result<ReceivedOperation<'a>, ConnectorError> {
        let compact = received
            .compact_files()
            .ok_or_else(|| internal("Expected compact write receiving"))?;
        let (operation, journal) = self.adopt_compact_operation(handle, compact, context)?;
        let recipe = WriteRecipe::freeze(handle, &[], documents).and_then(|mut recipe| {
            let validation = recipe
                .session_protocol
                .row_delta_isolation()
                .map(|isolation| {
                    let scope = received
                        .row_conflict_scope(&recipe.source)?
                        .ok_or_else(|| {
                            internal("Iceberg row-delta operation has no original conflict scope")
                        })?;
                    Ok::<_, ConnectorError>((isolation, scope))
                })
                .transpose()?;
            let files = received
                .take_compact_files()
                .expect("adopted compact files");
            let stage = usize::from(recipe.keyed_documents.is_some());
            recipe.compact_stage = Some(stage);
            recipe.output_facts.data_files = files.counts.data.files;
            recipe.output_facts.data_rows = files.counts.data.rows;
            if files.has_delete_effects() {
                if !recipe.fixed_mutation {
                    return Err(internal(
                        "Iceberg compact delete lost its fixed write endpoint",
                    ));
                }
                recipe.materialized =
                    Some(Arc::new(materialized::MaterializedDeleteStep::compact(
                        operation.clone(),
                        stage,
                        files.counts,
                    )));
                if recipe.source.format_version() == crate::iceberg::spec::FormatVersion::V2 {
                    recipe.output_facts.position_delete_files = files.counts.position_deletes.files;
                } else {
                    recipe.output_facts.deletion_vectors = files.counts.position_deletes.files;
                }
            }
            if let Some((isolation, scope)) = validation {
                let facts = crate::commit::row_delta_facts::extract(&files)?;
                let check = crate::commit::dependency::RowDeltaWindowCheck::new(
                    isolation,
                    Arc::new(scope),
                    Arc::clone(&facts.facts),
                    files.counts.data.files != 0,
                    files.counts.position_deletes.files != 0,
                );
                let source = recipe.materialized.as_ref().map(|step| {
                    let source: Arc<dyn crate::commit::dependency::RemovedDataFiles> = step.clone();
                    Arc::downgrade(&source)
                });
                operation
                    .hold_row_delta_validation(check, facts, source)
                    .map_err(|error| internal(error.to_string()))?;
            }
            operation
                .install_stage_new_files(
                    stage,
                    crate::commit::new_manifests::PendingNewFiles {
                        received: files,
                        source: Arc::clone(&recipe.source),
                        codec: super::super::codec::CompactFragmentCodec::for_adapter(
                            &self.adapter,
                        ),
                    },
                )
                .map_err(|error| internal(error.to_string()))?;
            Ok(recipe)
        });
        Ok(ReceivedOperation {
            operation,
            journal,
            recipe,
        })
    }

    pub(super) async fn dispatch_operation_async(
        &self,
        handle: &IcebergWriteCommitSession,
        prepared: ReceivedOperation<'_>,
        statistics: Vec<novarocks_spi::connector::StatisticsArtifactDraft>,
        publishes_documents: bool,
        context: &ConnectorRequestContext,
    ) -> WritePublicationDispatch {
        let ReceivedOperation {
            operation,
            journal,
            recipe,
        } = prepared;
        let recipe = match recipe {
            Ok(recipe) => recipe,
            Err(error) => {
                let error = if publishes_documents {
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
                return WritePublicationDispatch::settled(
                    self.reject_operation(handle, &operation, error).await,
                );
            }
        };
        let data = match recipe.preparer(&operation) {
            Ok(data) => data,
            Err(error) => {
                return WritePublicationDispatch::settled(
                    self.reject_operation(handle, &operation, error).await,
                );
            }
        };
        let policy = match RetryPolicy::from_properties(recipe.source.properties()) {
            Ok(policy) => policy,
            Err(error) => {
                return WritePublicationDispatch::settled(
                    self.reject_operation(handle, &operation, format_error_to_connector(error))
                        .await,
                );
            }
        };
        let template =
            match RecoveryTemplate::from_handle(&self.descriptor, self.key.incarnation, handle) {
                Ok(template) => template,
                Err(error) => {
                    return WritePublicationDispatch::settled(
                        self.reject_operation(handle, &operation, error).await,
                    );
                }
            };
        let preflight_journal = journal.clone();
        let receipt_partitioning = handle.repartition().map(|r| r.committed().clone());
        let receipt_output_facts = recipe.output_facts;
        let retained_evidence = handle.recovery_evidence_slot();
        let managed_effect = recipe
            .managed_overwrite
            .as_ref()
            .map(|data| data.effect.clone());
        let preflight = Arc::new(
            move |request: &FrozenRequest, operation: &IcebergCommitOperation| {
                if let Some(effect) = &managed_effect {
                    effect.verify_request(request).map_err(format_error)?;
                }
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
            expected_table_uuid: None,
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
            let data: &dyn Preparer = match &recipe.managed_overwrite {
                Some(managed) => managed,
                None => data.as_ref(),
            };
            let carried_statistics = if matches!(
                recipe.session_protocol,
                WriteSessionCommitProtocol::RewriteFiles
            ) {
                let Some((stage, author)) = recipe
                    .compact_stage
                    .zip(handle.rewrite_payload_byte_author())
                else {
                    let failure = format_error(internal(
                        "Iceberg rewrite statistics lost its original Replace stage or byte author",
                    ));
                    return attempt::rejected(
                        &run_operation,
                        attempt::before_dispatch_failure(&failure),
                    )
                    .await;
                };
                Some(
                    crate::commit::statistics_carry::CarryForwardStatisticsPreparer::new(
                        &run_operation,
                        stage,
                        author,
                    ),
                )
            } else {
                None
            };
            preparers.push(data);
            if let Some(carry) = &carried_statistics {
                preparers.push(carry);
            }
            preparers.push(&stats);
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
            return WritePublicationDispatch {
                outcome,
                cleanup: None,
            };
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
        WritePublicationDispatch {
            outcome: WritePublicationVerdict::KnownCommitted {
                cleanup,
                effect,
                receipt: enriched,
                finalization,
            },
            cleanup: Some((operation, journal)),
        }
    }

    fn adopt_received_operation(
        &self,
        handle: &IcebergWriteCommitSession,
        validated: &[ValidatedFragment<'_>],
        context: &ConnectorRequestContext,
    ) -> Result<
        (
            IcebergCommitOperation,
            PublicationJournal<ConnectorWriteReceipt>,
        ),
        ConnectorError,
    > {
        let objects = validated
            .iter()
            .filter_map(|entry| entry.fragment.new_object_path())
            .map(ObjectIdentity::new)
            .collect::<crate::iceberg::Result<BTreeSet<_>>>()
            .map_err(|error| corrupt(error.to_string()))?;
        self.adopt_operation_objects(handle, objects, context)
    }

    fn adopt_compact_operation(
        &self,
        handle: &IcebergWriteCommitSession,
        compact: &super::super::receiving::ReceivedCompactFiles,
        context: &ConnectorRequestContext,
    ) -> Result<
        (
            IcebergCommitOperation,
            PublicationJournal<ConnectorWriteReceipt>,
        ),
        ConnectorError,
    > {
        let objects = compact
            .entries()
            .filter_map(|entry| entry.addition.as_ref())
            .map(|addition| ObjectIdentity::from_shared_path(Arc::clone(&addition.object_path)))
            .collect::<crate::iceberg::Result<BTreeSet<_>>>()
            .map_err(|error| corrupt(error.to_string()))?;
        self.adopt_operation_objects(handle, objects, context)
    }

    fn adopt_operation_objects(
        &self,
        handle: &IcebergWriteCommitSession,
        objects: BTreeSet<ObjectIdentity>,
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
        for object in objects {
            operation
                .adopt_session_data(object)
                .map_err(|error| internal(error.to_string()))?;
        }
        // Publish the original owner only after adoption has completed, with
        // no asynchronous boundary or created side effect in between.
        handle.retain_publication(operation.clone(), journal.clone())?;
        Ok((operation, journal))
    }

    pub(super) async fn cleanup_received_set_async(
        &self,
        handle: &IcebergWriteCommitSession,
        received: IcebergReceiving,
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
        // This is the original confirmed receiver won by the release or
        // refused finish. No ref/live-set scan or old reference grants deletion.
        let (operation, _) = if let Some(compact) = received.compact_files() {
            self.adopt_compact_operation(handle, compact, context)?
        } else {
            let validated = validate_received_set(handle, &self.adapter, &received)?;
            self.adopt_received_operation(handle, &validated, context)?
        };
        drop(received);
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
        handle: &IcebergWriteCommitSession,
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
        handle: &IcebergWriteCommitSession,
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
        handle: &IcebergWriteCommitSession,
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
        };
        payload.validate_facts(self)?;
        let bytes = serde_json::to_vec(&payload).map_err(|error| internal(error.to_string()))?;
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
    pub(super) fn names_publication(&self, snapshot: i64, parent: Option<i64>) -> bool {
        self.published_snapshot_id == Some(snapshot)
            && self.parent_snapshot_id == parent
            && self.base_snapshot_id == parent
    }

    pub(super) fn validate(
        &self,
        descriptor: &ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        handle: &IcebergWriteCommitSession,
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
        if uuid::Uuid::from_bytes(self.attempt.nonce).get_version_num() != 7 {
            return Err(corrupt("Iceberg recovery attempt has no UUIDv7 nonce"));
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
                novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
                Arc::new(novarocks_fs::TokioFileIoSpawner::new(runtime.clone())),
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
            let added = self
                .operation
                .artifacts()
                .unwrap()
                .into_iter()
                .filter_map(|record| {
                    matches!(
                        record.origin,
                        ArtifactOrigin::Received {
                            publishes: ReceivedPublication::ViaManifest
                        }
                    )
                    .then(|| {
                        AddedContent::new_logical_data(
                            crate::commit::overwrite::preparer_tests::data(
                                record.object.path(),
                                1,
                                crate::iceberg::spec::Struct::empty(),
                            ),
                            self.metadata.default_partition_spec_id(),
                        )
                        .unwrap()
                    })
                })
                .collect();
            self.intent_with_files(added)
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
                protocol: crate::commit::model::CommitProtocol::Legacy,
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
            // Independent path segments exercise long, unrelated owned object locations.
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
        commit: bool,
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
        async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            if self.commit {
                return CatalogOutcome::committed(
                    CommitProof::applied(request.ref_snapshot_after(request.target_ref())),
                    ExternalMutationEffect::Applied,
                );
            }
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
            commit: false,
        }
    }

    #[tokio::test]
    async fn large_owned_ledger_publishes_with_bounded_evidence_and_keeps_received_objects() {
        let fixture = Fixture::new();
        let objects: Vec<_> = (0..800).map(|_| fixture.adopt_entropy()).collect();
        let mut publisher = publisher(&fixture);
        publisher.commit = true;
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
            matches!(report.publication, PublicationOutcome::Committed(_)),
            "{report:?}"
        );
        assert_eq!(publisher.dispatches.load(Ordering::SeqCst), 1);
        let checked = publisher.checked.lock().unwrap().clone().unwrap();
        assert!(
            checked.provider_payload().len()
                <= novarocks_spi::connector::MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES
        );
        let payload: RecoveryPayload = serde_json::from_slice(checked.provider_payload()).unwrap();
        payload.validate_facts(&fixture.template).unwrap();
        assert!(matches!(
            report.cleanup,
            IcebergCleanupReport::Complete { .. }
        ));
        assert!(fixture.operation.artifacts().unwrap().is_empty());
        assert!(objects.iter().all(|object| {
            std::path::Path::new(object.path().strip_prefix("file://").unwrap()).exists()
        }));
    }

    #[tokio::test]
    async fn recovery_evidence_size_is_independent_of_owned_object_count() {
        use crate::commit::staging::StagingEngine;
        let mut normalized = None;
        for count in [1, 1221] {
            let fixture = Fixture::new();
            let sessions: Vec<_> = (0..count).map(|_| fixture.adopt_random_file()).collect();
            let old_attempt = fixture.operation.begin_attempt().unwrap();
            let old_object = old_attempt.allocate(ArtifactKind::Statistics).unwrap();
            let operation_path = fixture.directory.path().join("operation-metadata.json");
            std::fs::write(&operation_path, b"operation metadata").unwrap();
            let operation_object =
                ObjectIdentity::new(format!("file://{}", operation_path.display())).unwrap();
            fixture
                .operation
                .adopt_operation_reference(operation_object.clone())
                .unwrap();
            let intent = fixture.intent();
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
            let request = engine.freeze().unwrap();
            let evidence = fixture
                .template
                .encode(&request, &fixture.operation)
                .unwrap();
            let payload: RecoveryPayload =
                serde_json::from_slice(evidence.provider_payload()).unwrap();
            payload.validate_facts(&fixture.template).unwrap();
            assert_eq!(
                serde_json::to_vec(&payload).unwrap(),
                evidence.provider_payload().as_ref()
            );
            assert!(
                evidence.provider_payload().len()
                    <= novarocks_spi::connector::MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES
            );
            let records = fixture.operation.artifacts().unwrap();
            assert_eq!(records.len(), count + 4);
            assert!(records.iter().any(|record| record.object == old_object
                && record.write_state == crate::commit::model::ArtifactWriteState::Allocated));
            assert!(
                records
                    .iter()
                    .any(|record| record.object == operation_object)
            );
            let mut value = serde_json::to_value(&payload).unwrap();
            for field in [
                "artifacts",
                "attempt_owned",
                "operation_references",
                "session_references",
            ] {
                assert!(value.get(field).is_none());
            }
            // These fields differ only because the two isolated owners have fresh identities.
            value["table_uuid"] = serde_json::json!("table-identity");
            value["staging_dir"] = serde_json::json!("staging-identity");
            value["metadata_location"] = serde_json::json!("base-identity");
            value["attempt"]["nonce"] = serde_json::to_value([0_u8; 16]).unwrap();
            value["published_snapshot_id"] = serde_json::json!(1);
            let bytes = serde_json::to_vec(&value).unwrap();
            match &normalized {
                None => normalized = Some(bytes),
                Some(expected) => assert_eq!(
                    &bytes, expected,
                    "owned file count cannot enlarge recovery facts"
                ),
            }
            assert!(sessions.iter().all(|object| {
                std::path::Path::new(object.path().strip_prefix("file://").unwrap()).exists()
            }));
        }
    }

    #[tokio::test]
    async fn unknown_envelope_preserves_old_attempt_and_complete_ownership_without_cleanup() {
        let fixture = Fixture::new();
        let session_object = fixture.adopt(0);
        let old_attempt = fixture.operation.begin_attempt().unwrap();
        let old_object = old_attempt.allocate(ArtifactKind::Statistics).unwrap();
        let operation_path = fixture.directory.path().join("operation-metadata.json");
        std::fs::write(&operation_path, b"operation metadata").unwrap();
        let operation_object =
            ObjectIdentity::new(format!("file://{}", operation_path.display())).unwrap();
        fixture
            .operation
            .adopt_operation_reference(operation_object.clone())
            .unwrap();
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
        let payload: RecoveryPayload = serde_json::from_slice(evidence.provider_payload()).unwrap();
        payload.validate_facts(&fixture.template).unwrap();
        assert_eq!(payload.operation_id, fixture.operation.token().to_bytes());
        assert_eq!(
            payload.attempt.ordinal,
            old_attempt.attempt_token().ordinal() + 1
        );
        let records = fixture.operation.artifacts().unwrap();
        let old = records
            .iter()
            .find(|record| record.object == old_object)
            .unwrap();
        assert_eq!(
            old.write_state,
            crate::commit::model::ArtifactWriteState::Allocated
        );
        assert!(
            matches!(old.origin, ArtifactOrigin::Produced { attempt, .. }
            if attempt == old_attempt.attempt_token())
        );
        assert!(
            records
                .iter()
                .any(|record| record.object == operation_object
                    && matches!(
                        record.origin,
                        ArtifactOrigin::Received {
                            publishes: ReceivedPublication::WithRequest
                        }
                    )
                    && record.write_state == crate::commit::model::ArtifactWriteState::Written)
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
        tampered.document_record_digest = Some([1; 32]);
        assert!(tampered.validate_facts(&fixture.template).is_err());
        let mut old_version = payload.clone();
        old_version.version -= 1;
        assert!(old_version.validate_facts(&fixture.template).is_err());
        let mut json = serde_json::to_value(&payload).unwrap();
        json["deleted_objects"] = serde_json::json!([]);
        assert!(serde_json::from_value::<RecoveryPayload>(json).is_err());
        let mut old_envelope = serde_json::to_value(&payload).unwrap();
        old_envelope["artifacts"] = serde_json::json!([]);
        assert!(serde_json::from_value::<RecoveryPayload>(old_envelope).is_err());
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
            protocol: crate::commit::model::CommitProtocol::Legacy,
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
    async fn keyed_publication_accepts_large_attempt_properties_before_writing_any_manifest() {
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
        assert!(
            properties
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
                > 256 * 1024
        );
        base = base
            .into_builder(None)
            .set_properties(properties.clone())
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let documents = publication(document("new-binding", 1));
        let version = &documents.versions[0];
        let version_key = crate::document_storage::keyed::keys::version_key(version.identity());
        let version_value =
            crate::document_storage::keyed::envelope::encode_version(version).unwrap();
        let root_key = crate::document_storage::keyed::keys::root_key("novarocks.test");
        let root_value = version.identity().to_hex();
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
            .stage(&KeyedPublicationPreparer(documents))
            .await
            .expect("large existing private properties do not prevent keyed preparation");
        for (key, value) in properties {
            assert_eq!(engine.metadata().properties().get(&key), Some(&value));
        }
        assert_eq!(
            engine.metadata().properties().get(&version_key),
            Some(&version_value)
        );
        assert_eq!(
            engine.metadata().properties().get(&root_key),
            Some(&root_value)
        );
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
        let frozen = engine.freeze().unwrap();
        assert_eq!(frozen.artifacts().references().len(), 1);
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
