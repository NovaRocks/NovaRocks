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

//! The frontend-only Iceberg write session control.
//!
//! `begin_write` completes admission and freezes the recipe with no external
//! side effect. `finish_write` validates a *complete* prepared write set and
//! performs exactly one external snapshot commit. `abort_write` is the
//! known-uncommitted release, and `reconcile_write` resolves an unknown
//! outcome by read-only adjudication against a durable snapshot marker.
//!
//! The commit verdicts are the provider's existing ones and are deliberately
//! not softened here:
//!
//! * a definite catalog refusal is `KnownUncommitted` and authorizes cleanup;
//! * a proven publication is `KnownCommitted`;
//! * a finalization failure after a proven publication stays `KnownCommitted`
//!   with a failed finalization, never a retryable commit;
//! * anything else — a timeout, a reset connection, a runtime-bridge failure —
//!   is `CommitUnknown`, keeps every staged object in place, and returns
//!   recovery evidence. Marker absence during reconciliation is *not* proof of
//!   non-commit, so an unresolved session stays unknown.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use novarocks_spi::connector::write_stack::session::{
    ConnectorWriteBeginRequest, ConnectorWriteFinishRequest, ConnectorWriteSessionAbortRequest,
    ConnectorWriteSessionFlavor, ConnectorWriteSessionPlan, ConnectorWriteSessionReconcileRequest,
    ConnectorWriteTargetPlan,
};
use novarocks_spi::connector::write_stack::{
    ConnectorManagedPublicationShape, ConnectorPreparedWriteSet, WriteStatisticsContract,
    WriteTargetOrdinal,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorDistributedRewriteShape, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorManagedPublicationIntent, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorMutationOperationId, ConnectorProviderBindingKey,
    ConnectorRequestContext, ConnectorWriteAbortOutcome, ConnectorWriteBaseVersion,
    ConnectorWriteFieldBinding, ConnectorWriteFieldRequest, ConnectorWriteFieldToken,
    ConnectorWriteInputRequest, ConnectorWriteInputShape, ConnectorWriteReceipt,
    ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome, ProviderBindingEpoch, StatisticsArtifactIdentity,
    StatisticsRequiredAggregation, StatisticsScanColumn,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::catalog::CatalogTransactionStart;
use crate::catalog::error::CatalogOutcome;
use crate::catalog::transaction::{TransactionIdentity, TransactionRequest};
use crate::commit::write_stack::domain::{
    IcebergArtifactPartition, IcebergCommitArtifact, IcebergCommitFragment, IcebergCommitHandle,
    IcebergContentRange, IcebergDataBranchRecipe, IcebergEmptyWriteDecision,
    IcebergManagedPublicationFacts, IcebergManagedPublicationProvenance, IcebergWriteFlavor,
    IcebergWriteSessionId, IcebergWriteSessionState, IcebergWriteTableFacts, IcebergWriterOutput,
    corrupt, invalid,
};
use crate::commit::write_stack::flavor::{
    IcebergFrozenRewriteBranch, IcebergSessionFlavorPlan, IcebergSessionMaterial,
    plan_copy_on_write_branches, plan_distributed_rewrite_branches,
    plan_managed_publication_branches, plan_ordinary_branches, plan_row_mutation_branches,
};
use crate::commit::write_stack::old_delete::{
    IcebergOldDeleteArtifactRef, IcebergOldDeleteMergeTarget, IcebergStorageRoute,
};
use crate::commit::write_stack::planning::{IcebergBranchSessionPlanInput, plan_branch_session};
use crate::commit::write_stack::runtime::IcebergWriteAdapter;
use crate::commit::{
    CommitOpKind, CommitServiceError, CowUpdateRewriteSet, CowUpdateTouchedFile,
    IcebergCommitCollector, RunInput, WrittenFile, run_iceberg_commit,
};
use crate::iceberg::spec::{DataContentType, DataFileFormat, TableMetadata};
use crate::iceberg::transaction::ApplyTransactionAction;
use crate::metadata_context::IcebergMetadataContext;
use crate::write_descriptor::decode_partition_descriptor;

/// The snapshot summary property that makes a session's publication provable
/// after an unknown outcome. Without a durable marker, reconciliation could
/// only guess, and a guess would either double-commit or strand data.
pub const ICEBERG_WRITE_SESSION_MARKER_PROPERTY: &str = "novarocks.write.session.v1";

/// The evidence schema version this control produces and accepts.
pub const ICEBERG_WRITE_SESSION_EVIDENCE_VERSION: u16 = 1;

/// The operation-kind tag on the evidence envelope.
pub const ICEBERG_WRITE_SESSION_OPERATION_KIND: &str = "iceberg.connector_write_session.v1";

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
        .with_retryable_before_progress()
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

fn failure(
    kind: ConnectorMutationFailureKind,
    message: impl Into<String>,
) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(kind, message.into())
}

/// The canonical provider payload inside a reconciliation evidence envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergWriteSessionEvidenceV1 {
    version: u16,
    session_id: String,
    table_ident: String,
    target_ref: String,
    op_kind: String,
    base_snapshot_id: Option<i64>,
    base_sequence_number: i64,
    staging_dir: String,
    manifest_cleanup_token: Option<String>,
}

/// The frontend-only Iceberg write authority of one exact catalog generation.
// Design: ADR-0135 (docs/adr/ADR-0135-ordinary-aggregate-statistics-dataflow.md)
pub struct IcebergWriteSessionControl {
    key: ConnectorProviderBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    adapter: IcebergWriteAdapter,
    runtime: Arc<IcebergMetadataContext>,
}

impl IcebergWriteSessionControl {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        catalog_handle: CatalogHandle,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Self {
        let key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        let adapter = crate::commit::write_stack::runtime::build_write_adapter(
            descriptor.clone(),
            catalog_handle,
        );
        Self {
            key,
            descriptor,
            adapter,
            runtime,
        }
    }

    pub const fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    pub const fn runtime(&self) -> &Arc<IcebergMetadataContext> {
        &self.runtime
    }
}

/// Reject a request whose attempt is already cancelled or past its deadline,
/// before any external work starts.
pub(crate) fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Iceberg write request was cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg write request deadline elapsed",
        ));
    }
    Ok(())
}

/// One typed fragment resolved against the sealed target set.
#[derive(Debug)]
pub(crate) struct ValidatedFragment<'a> {
    ordinal: WriteTargetOrdinal,
    fragment: &'a IcebergCommitFragment,
}

/// Validate a complete prepared write set against a sealed session.
///
/// Every check here is a fail-closed precondition of the single external
/// commit: a fragment that names an unsealed target, a branch its target does
/// not drive, a duplicate path, a partition spec the table does not have, or a
/// delete artifact whose data file is owned by another target would each make
/// the resulting snapshot wrong in a way no later step could detect.
pub(crate) fn validate_prepared_set<'a>(
    handle: &IcebergCommitHandle,
    adapter: &IcebergWriteAdapter,
    prepared: &'a ConnectorPreparedWriteSet,
) -> Result<Vec<ValidatedFragment<'a>>, ConnectorError> {
    let sealed = handle
        .targets()
        .iter()
        .map(|target| (target.ordinal(), target.branch()))
        .collect::<BTreeMap<_, _>>();
    let mut paths = BTreeSet::new();
    let mut delete_artifact_owner: BTreeMap<&str, WriteTargetOrdinal> = BTreeMap::new();
    let mut validated = Vec::with_capacity(prepared.fragments().len());

    for (ordinal, neutral) in prepared.fragments() {
        let fragment = adapter.commit_fragment(neutral)?;
        let branch = sealed.get(ordinal).copied().ok_or_else(|| {
            invalid(format!(
                "Iceberg commit fragment names write target {} which this session never sealed",
                ordinal.get()
            ))
        })?;
        if fragment.branch() != branch {
            return Err(invalid(format!(
                "Iceberg commit fragment describes a {} artifact but write target {} drives the {} branch",
                fragment.branch().as_str(),
                ordinal.get(),
                branch.as_str()
            )));
        }
        if !paths.insert(fragment.path()) {
            return Err(corrupt(format!(
                "Iceberg prepared write set repeats staged artifact {}",
                fragment.path()
            )));
        }
        if let Some(referenced) = fragment.referenced_data_file() {
            // Exactly one delete artifact per referenced data file: Iceberg
            // permits one deletion vector per data file, and two merged
            // position-delete files would each claim to be the whole truth.
            if delete_artifact_owner.insert(referenced, *ordinal).is_some() {
                return Err(corrupt(format!(
                    "Iceberg prepared write set stages more than one delete artifact for data file {referenced}"
                )));
            }
            let owner = handle.delete_owner().get(referenced).ok_or_else(|| {
                invalid(format!(
                    "Iceberg delete artifact references data file {referenced}, which this session never routed"
                ))
            })?;
            if owner != ordinal {
                return Err(invalid(format!(
                    "Iceberg delete artifact for data file {referenced} was staged by write target {} but the session routed it to {}",
                    ordinal.get(),
                    owner.get()
                )));
            }
        }
        if fragment.metrics().record_count() == 0
            && matches!(fragment.artifact(), IcebergCommitArtifact::DataFile(_))
        {
            return Err(invalid(format!(
                "Iceberg staged data file {} reports zero rows",
                fragment.path()
            )));
        }
        validated.push(ValidatedFragment {
            ordinal: *ordinal,
            fragment,
        });
    }
    Ok(validated)
}

/// Verify that every delete artifact superseded exactly the old references the
/// session froze for its data file.
///
/// This is the commit-side half of the D10 contract: the backend proved it read
/// the frozen references, and the frontend proves the artifact it is about to
/// commit accounts for all of them.
///
/// The check runs over the *staged* artifacts, never over the frozen map. The
/// frozen map is a superset by construction: `freeze_old_delete_references`
/// walks every data file of the base snapshot, because at `begin_write` the
/// frontend cannot know which of them the statement's predicate will match. A
/// merge-on-read writer stages a delete artifact only for the data files it
/// actually deleted a row from, and the commit action carries every other data
/// file's delete manifest through verbatim, so a frozen data file with no
/// staged artifact keeps its existing deletes rather than losing them.
/// Demanding one artifact per frozen data file would therefore refuse every
/// `DELETE` that does not happen to touch the whole table.
pub(crate) fn validate_merged_old_references(
    plans: &BTreeMap<WriteTargetOrdinal, BTreeMap<String, Vec<String>>>,
    validated: &[ValidatedFragment<'_>],
) -> Result<(), ConnectorError> {
    for entry in validated {
        let Some(referenced) = entry.fragment.referenced_data_file() else {
            continue;
        };
        let frozen = plans
            .get(&entry.ordinal)
            .and_then(|files| files.get(referenced))
            .ok_or_else(|| {
                invalid(format!(
                    "Iceberg delete artifact references data file {referenced}, which write target {} never froze",
                    entry.ordinal.get()
                ))
            })?;
        let observed = entry.fragment.merged_old_references();
        if observed != frozen.as_slice() {
            return Err(corrupt(format!(
                "Iceberg delete artifact for {referenced} merged {} old references but the session froze {}",
                observed.len(),
                frozen.len()
            )));
        }
    }
    Ok(())
}

fn validate_statistics_artifacts(
    handle: &IcebergCommitHandle,
    artifacts: Vec<novarocks_spi::connector::write_stack::WriteStatisticsArtifact>,
) -> Result<Vec<novarocks_spi::connector::StatisticsArtifactDraft>, ConnectorError> {
    let expected = handle.statistics_expectations();
    let expected_pairs = expected
        .iter()
        .flat_map(|(target, identities)| {
            identities
                .iter()
                .cloned()
                .map(|identity| (*target, identity))
        })
        .collect::<BTreeSet<_>>();
    if artifacts.len() != expected_pairs.len() {
        return Err(invalid(
            "Iceberg write statistics artifact count does not match the sealed expectation set",
        ));
    }

    let mut observed = BTreeSet::new();
    let mut by_identity: BTreeMap<StatisticsArtifactIdentity, Vec<Bytes>> = BTreeMap::new();
    for artifact in artifacts {
        let (target, draft) = artifact.into_parts();
        let key = (target, draft.identity().clone());
        if !expected_pairs.contains(&key) || !observed.insert(key) {
            return Err(invalid(
                "Iceberg write statistics artifacts contain an unknown or duplicate target identity",
            ));
        }
        if draft.identity().blob_type() != crate::iceberg::puffin::APACHE_DATASKETCHES_THETA_V1 {
            return Err(invalid(
                "Iceberg write statistics contract produced an unsupported blob type",
            ));
        }
        novarocks_connector_iceberg_functions::validate_compact_theta(draft.body())
            .map_err(|error| corrupt(error.to_string()))?;
        let estimate = novarocks_connector_iceberg_functions::estimate_compact_theta(draft.body())
            .map_err(|error| corrupt(error.to_string()))?;
        validate_theta_properties(draft.properties(), estimate)?;
        by_identity
            .entry(draft.identity().clone())
            .or_default()
            .push(draft.body().clone());
    }
    if observed != expected_pairs {
        return Err(invalid(
            "Iceberg write statistics artifacts do not exactly match the sealed expectation set",
        ));
    }

    by_identity
        .into_iter()
        .map(|(identity, bodies)| {
            let body = novarocks_connector_iceberg_functions::union_compact_theta(
                bodies.iter().map(Bytes::as_ref),
            )
            .map_err(|error| corrupt(error.to_string()))?;
            normalized_theta_artifact(identity, Bytes::from(body))
        })
        .collect()
}

fn validate_theta_properties(
    properties: &BTreeMap<String, String>,
    estimate: f64,
) -> Result<(), ConnectorError> {
    if properties
        .keys()
        .any(|property| property != crate::stats_loader::NDV_PROPERTY)
    {
        return Err(invalid(
            "Iceberg Theta write statistics contain an unsupported property",
        ));
    }
    if let Some(rendered) = properties.get(crate::stats_loader::NDV_PROPERTY) {
        let supplied = rendered
            .parse::<f64>()
            .map_err(|_| invalid("Iceberg Theta write statistics ndv property is not numeric"))?;
        if !supplied.is_finite() || supplied < 0.0 || supplied.to_bits() != estimate.to_bits() {
            return Err(invalid(
                "Iceberg Theta write statistics ndv property does not match its body",
            ));
        }
    }
    Ok(())
}

fn normalized_theta_artifact(
    identity: StatisticsArtifactIdentity,
    body: Bytes,
) -> Result<novarocks_spi::connector::StatisticsArtifactDraft, ConnectorError> {
    let estimate = novarocks_connector_iceberg_functions::estimate_compact_theta(&body)
        .map_err(|error| corrupt(error.to_string()))?;
    novarocks_spi::connector::StatisticsArtifactDraft::try_new(
        identity.input_fields().to_vec(),
        identity.blob_type(),
        body,
        BTreeMap::from([(
            crate::stats_loader::NDV_PROPERTY.to_string(),
            estimate.to_string(),
        )]),
    )
}

fn cleanup_eager_logs(
    runtime: &IcebergMetadataContext,
    fs: crate::opendal::Operator,
    mapper: crate::commit::CleanupPathMapper,
    logs: Vec<Arc<crate::commit::abort::AbortLog>>,
) {
    let _ = runtime.resources().catalog_runtime().block_on(async move {
        for log in logs {
            for error in log
                .cleanup_with_path_mapper(&fs, |path| mapper(path))
                .await
            {
                tracing::warn!(path = %error.path, source = ?error.source, "eager Iceberg cleanup failed");
            }
        }
    });
}

pub(crate) fn eager_conflict_backoff(
    runtime: &IcebergMetadataContext,
    context: &ConnectorRequestContext,
    next_attempt: usize,
) -> Result<(), ConnectorError> {
    validate_context(context)?;
    let delay = std::time::Duration::from_millis(
        crate::commit::retry::COMMIT_RETRY_BACKOFF_MS[next_attempt - 1],
    );
    if context
        .deadline()
        .saturating_duration_since(std::time::Instant::now())
        <= delay
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg write deadline does not permit another conflict attempt",
        ));
    }
    let end = std::time::Instant::now() + delay;
    while std::time::Instant::now() < end {
        validate_context(context)?;
        let step = end
            .saturating_duration_since(std::time::Instant::now())
            .min(std::time::Duration::from_millis(5));
        runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { tokio::time::sleep(step).await })
            .map_err(unavailable)?;
    }
    validate_context(context)
}

fn commit_proof_matches_eager_snapshot(
    proof: &crate::catalog::transaction::CommitProof,
    snapshot_id: i64,
    table_uuid: &str,
) -> bool {
    proof.snapshot_id == Some(snapshot_id)
        && proof
            .table_uuid
            .as_deref()
            .is_none_or(|observed| observed == table_uuid)
}

fn eager_proof_finalization(
    proof: &crate::catalog::transaction::CommitProof,
    snapshot_id: i64,
    table_uuid: &str,
) -> Option<ExternalMutationFinalization> {
    (!commit_proof_matches_eager_snapshot(proof, snapshot_id, table_uuid)).then(|| {
        ExternalMutationFinalization::Failed(failure(
            ConnectorMutationFailureKind::Internal,
            "Iceberg catalog commit proof does not match the eagerly staged snapshot",
        ))
    })
}

async fn artifacts_for_staged_snapshot(
    base: &crate::iceberg::table::Table,
    staged_metadata: &TableMetadata,
    target_ref: &str,
    op_kind: CommitOpKind,
    current: Vec<novarocks_spi::connector::StatisticsArtifactDraft>,
) -> Result<Vec<novarocks_spi::connector::StatisticsArtifactDraft>, ConnectorError> {
    if current.is_empty() || op_kind == CommitOpKind::Overwrite {
        return Ok(current);
    }
    let parent_id =
        crate::ref_snapshot::resolve_branch_head_snapshot_id(base.metadata(), target_ref)
            .map_err(invalid)?;
    let Some(parent_id) = parent_id else {
        return Ok(current);
    };
    let parent_is_empty = base
        .metadata()
        .snapshot_by_id(parent_id)
        .and_then(|snapshot| {
            snapshot
                .summary()
                .additional_properties
                .get("total-records")
        })
        .and_then(|value| value.parse::<u64>().ok())
        == Some(0);
    if parent_is_empty {
        return Ok(current);
    }
    let Some(parent_statistics) = base.metadata().statistics_for_snapshot(parent_id) else {
        // A non-empty parent without an artifact for a field cannot prove
        // whole-table coverage for that field after append.
        return Ok(Vec::new());
    };
    let parent_bodies = crate::stats_loader::StatsLoader::load_theta_bodies_from_file(
        &parent_statistics.statistics_path,
        base.file_io(),
    )
    .await
    .map_err(corrupt)?;
    let parent_snapshot = base
        .metadata()
        .snapshot_by_id(parent_id)
        .ok_or_else(|| corrupt(format!("Iceberg parent snapshot {parent_id} is absent")))?;
    let current_snapshot = staged_metadata
        .snapshot_by_id(
            crate::ref_snapshot::resolve_branch_head_snapshot_id(staged_metadata, target_ref)
                .map_err(corrupt)?
                .ok_or_else(|| corrupt("staged Iceberg snapshot is absent"))?,
        )
        .ok_or_else(|| corrupt("staged Iceberg snapshot is absent"))?;
    let parent_schema = parent_snapshot
        .schema(base.metadata())
        .map_err(|error| corrupt(format!("resolve Iceberg parent statistics schema: {error}")))?;
    let current_schema = current_snapshot
        .schema(staged_metadata)
        .map_err(|error| corrupt(format!("resolve staged Iceberg statistics schema: {error}")))?;

    let mut merged = Vec::new();
    for artifact in current {
        let [field_id] = artifact.identity().input_fields() else {
            return Err(corrupt(
                "Iceberg Theta write artifact must identify exactly one field",
            ));
        };
        let compatible = match (
            parent_schema.field_by_id(*field_id),
            current_schema.field_by_id(*field_id),
        ) {
            (Some(parent), Some(current)) => parent.field_type == current.field_type,
            _ => false,
        };
        let Some(parent_body) = compatible.then(|| parent_bodies.get(field_id)).flatten() else {
            continue;
        };
        let body = novarocks_connector_iceberg_functions::union_compact_theta([
            parent_body.as_slice(),
            artifact.body().as_ref(),
        ])
        .map_err(|error| corrupt(error.to_string()))?;
        merged.push(normalized_theta_artifact(
            artifact.identity().clone(),
            Bytes::from(body),
        )?);
    }
    Ok(merged)
}

/// Turn one validated fragment into the provider's physical commit input.
fn written_file_from_fragment(
    fragment: &IcebergCommitFragment,
    metadata: &TableMetadata,
) -> Result<WrittenFile, ConnectorError> {
    let partition = fragment.partition();
    let partition_values = decode_partition_values(partition, metadata)?;
    let metrics = fragment.metrics();
    let record_count = metrics.record_count();
    let file_size_in_bytes = metrics.file_size_in_bytes();
    let stats = metrics.column_stats().cloned().unwrap_or_default();
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

/// Immutable inputs that survive an eager OCC conflict.
///
/// Writer fragments have already materialized the data files and ordinary
/// aggregation has already materialized the statistic bodies before this
/// owner enters the catalog attempt loop. A fresh attempt may rebuild only
/// attempt-bound metadata; it must never turn either value back into a source
/// that can reopen the user's data files or recompute the aggregate.
#[derive(Clone)]
struct ReusableEagerWriteInputs {
    files: Arc<[WrittenFile]>,
    statistics: Arc<[novarocks_spi::connector::StatisticsArtifactDraft]>,
}

impl ReusableEagerWriteInputs {
    fn new(
        files: Vec<WrittenFile>,
        statistics: Vec<novarocks_spi::connector::StatisticsArtifactDraft>,
    ) -> Self {
        Self {
            files: files.into(),
            statistics: statistics.into(),
        }
    }

    fn install_files(&self, collector: &IcebergCommitCollector) {
        collector.inject_reusable_written_files(self.files.iter().cloned().collect());
    }

    fn statistics_for_attempt(&self) -> Vec<novarocks_spi::connector::StatisticsArtifactDraft> {
        self.statistics.iter().cloned().collect()
    }
}

fn permits_fresh_eager_attempt(
    outcome: &CatalogOutcome<crate::catalog::transaction::CommitProof>,
    attempt: usize,
    max_attempts: usize,
    has_repartition: bool,
) -> bool {
    matches!(
        outcome,
        CatalogOutcome::KnownUncommitted { failure }
            if failure.kind() == ConnectorMutationFailureKind::Conflict
                && attempt + 1 < max_attempts
                && !has_repartition
    )
}

/// The summary properties the single external commit stamps onto its snapshot.
///
/// Two things are recorded, and each exists because something later reads it
/// back off the snapshot rather than out of memory:
///
/// * the write-session marker, which is the only proof available to
///   reconciliation after an unknown outcome;
/// * a managed publication's durable provenance, whose publication id is what
///   the publication fence matches when it fast-forwards a staged refresh
///   (`commit::snapshot_matches_publication_marker`). A publication that
///   committed without it would strand its own snapshot.
///
/// `staged_data_rows` seeds the provenance row count. The commit action
/// replaces it with the committed snapshot's real `total-records`, so it is a
/// starting value rather than a claim.
pub(crate) fn session_snapshot_properties(
    handle: &IcebergCommitHandle,
    staged_data_rows: u64,
) -> Result<BTreeMap<String, String>, ConnectorError> {
    let mut properties = BTreeMap::new();
    properties.insert(
        ICEBERG_WRITE_SESSION_MARKER_PROPERTY.to_string(),
        handle.session_id().to_string(),
    );
    if let Some(publication) = handle.publication() {
        for (key, value) in publication
            .provenance()
            .to_summary_properties(publication.technique(), staged_data_rows)?
        {
            if properties.insert(key, value).is_some() {
                return Err(internal(
                    "Iceberg write session marker conflicts with managed snapshot properties",
                ));
            }
        }
    }
    Ok(properties)
}

/// The replacement record one copy-on-write commit applies.
///
/// The join key is the write target ordinal and nothing else. Branch `i`
/// replaces exactly the file its frozen recipe named, and the artifacts the
/// backends staged for target `i` are that file's replacement — so a
/// statement that rewrote three files produces three touched files, each with
/// its own artifacts, rather than one merged claim. No cohort, operation,
/// execution, or attempt identity takes part.
///
/// The matched row ids come off the frozen recipe rather than off anything the
/// writers reported: their minimum becomes the replacement manifest's
/// `first_row_id`, which is what stops the v3 manifest-list writer allocating
/// fresh `_row_id`s for rows that already have them.
pub(crate) fn cow_update_rewrite_set(
    handle: &IcebergCommitHandle,
    staged: &[(WriteTargetOrdinal, WrittenFile)],
) -> Result<Option<CowUpdateRewriteSet>, ConnectorError> {
    use crate::commit::write_stack::copy_on_write::IcebergCowBranchInput;

    if handle.commit_op_kind() != CommitOpKind::CowUpdate {
        return Ok(None);
    }
    let base_snapshot_id = handle
        .table()
        .base_snapshot_id()
        .ok_or_else(|| invalid("Iceberg copy-on-write commit requires a frozen base snapshot"))?;
    let staged_for = |ordinal: WriteTargetOrdinal| {
        staged
            .iter()
            .filter(move |(staged_ordinal, _)| *staged_ordinal == ordinal)
            .map(|(_, file)| file)
    };
    let mut touched_data_files = Vec::new();
    let mut appended_files = Vec::new();
    let mut updated_row_ids = BTreeSet::new();
    for (index, branch) in handle.copy_on_write().iter().enumerate() {
        let ordinal = WriteTargetOrdinal::try_new(
            u32::try_from(index)
                .map_err(|_| internal("Iceberg copy-on-write branch ordinal overflowed"))?,
        )?;
        match branch {
            IcebergCowBranchInput::Rewrite {
                old_file,
                matched_row_ids,
            } => {
                updated_row_ids.extend(matched_row_ids.iter().copied());
                touched_data_files.push(CowUpdateTouchedFile {
                    old_file: old_file.clone(),
                    new_files: staged_for(ordinal).map(|file| file.path.clone()).collect(),
                    row_ids: matched_row_ids.clone(),
                });
            }
            IcebergCowBranchInput::Append => {
                appended_files.extend(staged_for(ordinal).cloned());
            }
        }
    }
    Ok(Some(CowUpdateRewriteSet {
        base_snapshot_id,
        target_table_uuid: handle.table().table_uuid().to_string(),
        updated_row_ids: updated_row_ids.into_iter().collect(),
        touched_data_files,
        appended_files,
    }))
}

/// The frozen input file set a selected-rewrite commit replaces.
///
/// Present exactly when the session commits as a rewrite. The set comes off the
/// session, not off the fragments: the commit action asserts that the frozen
/// inputs are still live and retires all of them in one snapshot, and a group
/// whose rows were all compacted away stages no artifact at all while still
/// having to be retired.
///
/// The kind comes off the session's flavor, which the frozen operation decided
/// before any group was cut: a data rewrite replaces the group's data files and
/// the delete artifacts they owned, a position-delete rewrite replaces exactly
/// the delete artifacts its groups selected. Deriving it from the staged
/// artifacts instead would let a rewrite that produced nothing retire the wrong
/// half of its frozen set.
pub(crate) fn selected_rewrite_files(
    handle: &IcebergCommitHandle,
) -> Option<crate::commit::selected_rewrite::SelectedRewriteFiles> {
    if handle.commit_op_kind() != CommitOpKind::SelectedRewrite {
        return None;
    }
    let kind = handle.flavor().selected_rewrite_kind()?;
    let frozen = handle.frozen_rewrite_input();
    Some(crate::commit::selected_rewrite::SelectedRewriteFiles {
        kind,
        data_paths: frozen.data_paths().clone(),
        delete_paths: frozen.delete_paths().clone(),
    })
}

fn decode_partition_values(
    partition: &IcebergArtifactPartition,
    metadata: &TableMetadata,
) -> Result<crate::iceberg::spec::Struct, ConnectorError> {
    decode_partition_descriptor(
        Some(partition.descriptor().clone()),
        partition.partition_spec_id(),
        metadata,
    )
    .map_err(|error| corrupt(error.detail_message()))
}

fn unsigned_counts(
    counts: &BTreeMap<i32, i64>,
) -> Result<std::collections::HashMap<i32, u64>, ConnectorError> {
    counts
        .iter()
        .map(|(field, value)| {
            u64::try_from(*value)
                .map(|value| (*field, value))
                .map_err(|_| corrupt("Iceberg staged artifact column statistic is negative"))
        })
        .collect()
}

/// Whether a field id is one of the row-lineage columns Iceberg reserves.
///
/// These ids live outside the table schema on purpose: `annotate_schema_from_scan_model`
/// stamps them onto the Arrow field directly so a lineage column can ride with
/// the data without being a table column.
const fn is_reserved_row_lineage_field(field_id: i32) -> bool {
    field_id == crate::row_lineage_synth::ICEBERG_RESERVED_FIELD_ID_ROW_ID
        || field_id
            == crate::row_lineage_synth::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
}

fn decode_bounds(
    bounds: &BTreeMap<i32, Vec<u8>>,
    metadata: &TableMetadata,
) -> Result<std::collections::HashMap<i32, crate::iceberg::spec::Datum>, ConnectorError> {
    let schema = metadata.current_schema();
    bounds
        .iter()
        // A row-lineage column is stamped with a reserved field id that is
        // deliberately absent from the table schema -- that absence is what
        // lets it travel beside the table's own columns. It therefore has no
        // schema type to decode a bound against, and Iceberg prunes on no
        // lineage column, so its bound is dropped rather than treated as a
        // corrupt artifact. Any other unknown id still is one.
        .filter(|(field_id, _)| !is_reserved_row_lineage_field(**field_id))
        .map(|(field_id, raw)| {
            let field = schema.field_by_id(*field_id).ok_or_else(|| {
                corrupt(format!(
                    "Iceberg staged artifact bound names unknown field id {field_id}"
                ))
            })?;
            let primitive = field.field_type.as_primitive_type().ok_or_else(|| {
                corrupt(format!(
                    "Iceberg staged artifact bound field id {field_id} is not a primitive"
                ))
            })?;
            let datum = crate::iceberg::spec::Datum::try_from_bytes(raw, primitive.clone())
                .map_err(|error| {
                    corrupt(format!(
                        "decode Iceberg staged artifact bound for field id {field_id}: {error}"
                    ))
                })?;
            Ok((*field_id, datum))
        })
        .collect()
}

impl IcebergWriteSessionControl {
    /// Perform the single external commit for one sealed session.
    ///
    /// The session's own state machine is the mutual exclusion: `begin_commit`
    /// latches, so a second `finish_write` cannot dispatch a second snapshot.
    pub fn commit_prepared_set(
        &self,
        handle: &IcebergCommitHandle,
        prepared: &ConnectorPreparedWriteSet,
        statistics: Vec<novarocks_spi::connector::StatisticsArtifactDraft>,
        frozen_old_references: &BTreeMap<WriteTargetOrdinal, BTreeMap<String, Vec<String>>>,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        validate_context(context)?;
        let validated = validate_prepared_set(handle, &self.adapter, prepared)?;
        validate_merged_old_references(frozen_old_references, &validated)?;

        if validated.is_empty()
            && handle.empty_write_decision() == IcebergEmptyWriteDecision::SkipExternalCommit
        {
            return settle_empty_write_without_commit(handle);
        }

        handle.begin_commit()?;
        let outcome = if matches!(
            handle.commit_op_kind(),
            CommitOpKind::FastAppend | CommitOpKind::Overwrite
        ) {
            self.dispatch_eager_commit(handle, &validated, statistics, context)
        } else {
            debug_assert!(statistics.is_empty());
            self.dispatch_commit(handle, &validated, context)
        };
        match &outcome {
            Ok(ExternalMutationOutcome::KnownCommitted { receipt, .. }) => {
                let snapshot_id = receipt
                    .committed_version()
                    .and_then(|version| version.snapshot_id())
                    .unwrap_or_default();
                handle.settle(IcebergWriteSessionState::KnownCommitted { snapshot_id })?;
            }
            Ok(ExternalMutationOutcome::KnownUncommitted { failure }) => {
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: failure.message().to_string(),
                })?;
            }
            Ok(ExternalMutationOutcome::CommitUnknown { failure, .. }) => {
                handle.settle(IcebergWriteSessionState::CommitUnknown {
                    message: failure.message().to_string(),
                    staging_dir: handle.staging_dir(),
                })?;
            }
            Err(error) => {
                // A refusal raised before dispatch never touched the catalog,
                // so the session stays uncommitted rather than unknown.
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: error.message().to_string(),
                })?;
            }
        }
        outcome
    }

    /// Seal a staged write into the receipt its publication will consume.
    ///
    /// The validation above the seal is the *same* validation the committing
    /// path runs, and deliberately so: a staged artifact that would have been
    /// rejected before a snapshot must be rejected before a publication too,
    /// or the create would carry files the ordinary path would never have
    /// admitted.
    ///
    /// What differs is everything after it. Nothing here touches the catalog,
    /// writes a manifest, or creates a snapshot; the validated artifacts are
    /// folded into the receipt payload and the session settles as sealed. The
    /// single `NotExist` assert-create that makes them visible stays with the
    /// staged-create capability that owns the target.
    ///
    /// An empty prepared write set reaches this the same way a non-empty one
    /// does. Whether an empty staged create publishes an empty table or refuses
    /// is a decision for the publication, which can see the target; it must not
    /// be inferred here from "there were no fragments".
    fn seal_staged_prepared_set(
        &self,
        handle: &IcebergCommitHandle,
        prepared: &ConnectorPreparedWriteSet,
        frozen_old_references: &BTreeMap<WriteTargetOrdinal, BTreeMap<String, Vec<String>>>,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        validate_context(context)?;
        let metadata = handle.staged_metadata().ok_or_else(|| {
            internal("Iceberg staged write session lost its frozen target metadata")
        })?;
        let validated = validate_prepared_set(handle, &self.adapter, prepared)?;
        validate_merged_old_references(frozen_old_references, &validated)?;

        // Claim the single terminal attempt before producing anything, so a
        // second finish cannot mint a second receipt for the same session.
        handle.begin_commit()?;
        let sealed = validated
            .iter()
            .map(|entry| {
                let file = written_file_from_fragment(entry.fragment, metadata)?;
                crate::commit::report::writer_report_from_written_file(&file, metadata)
                    .map_err(corrupt)
            })
            .collect::<Result<Vec<_>, ConnectorError>>();
        let sealed = match sealed {
            Ok(sealed) => sealed,
            Err(error) => {
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: error.message().to_string(),
                })?;
                return Err(error);
            }
        };
        let payload = match crate::write_codec::encode_writer_reports(&sealed, metadata) {
            Ok(payload) => payload,
            Err(error) => {
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: error.clone(),
                })?;
                return Err(internal(format!("seal staged Iceberg write set: {error}")));
            }
        };
        let receipt = match ConnectorWriteReceipt::try_new(payload) {
            Ok(receipt) => receipt,
            Err(error) => {
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: error.message().to_string(),
                })?;
                return Err(error);
            }
        };
        handle.settle(IcebergWriteSessionState::Sealed)?;
        // `NoOp` is the honest effect: this session applied nothing externally,
        // and it is `KnownCommitted` only in the sense the caller needs -- the
        // sealing is finished and its result is not in doubt.
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        })
    }

    fn dispatch_commit(
        &self,
        handle: &IcebergCommitHandle,
        validated: &[ValidatedFragment<'_>],
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let facts = handle.table();
        let physical = self
            .runtime
            .load_table_for_request(facts.namespace(), facts.table_name(), context)
            .map_err(|error| unavailable(error.to_string()))?;
        let table = physical.into_table();
        let metadata = table.metadata().clone();
        if metadata.uuid().to_string() != facts.table_uuid()
            || metadata.current_schema_id() != facts.schema_id()
            || metadata.default_partition_spec_id() != facts.default_partition_spec_id()
            || crate::ref_snapshot::resolve_branch_head_snapshot_id(&metadata, facts.target_ref())
                .ok()
                .flatten()
                != facts.base_snapshot_id()
        {
            return Err(invalid(
                "Iceberg write target no longer matches its exact sealed session",
            ));
        }

        // The staged artifacts were written under the generation the *writers*
        // were built against. Under an atomic partition replacement that is the
        // prospective one, whose new spec the loaded table does not have yet, so
        // interpreting the artifacts against the loaded metadata would fail to
        // resolve their own partition spec.
        let commit_metadata = handle
            .repartition()
            .map_or(&metadata, |prepared| prepared.prospective_metadata());

        let files = validated
            .iter()
            .map(|entry| written_file_from_fragment(entry.fragment, commit_metadata))
            .collect::<Result<Vec<_>, _>>()?;

        let table_ident =
            crate::iceberg::TableIdent::from_strs([facts.namespace(), facts.table_name()])
                .map_err(|error| invalid(error.to_string()))?;
        let op_kind = handle.commit_op_kind();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                op_kind,
                table_ident,
                facts.base_snapshot_id(),
                metadata.last_sequence_number(),
                commit_metadata.current_schema().clone(),
                commit_metadata.default_partition_spec().clone(),
                handle.staging_dir(),
            )
            .with_table_metadata(commit_metadata.clone()),
        );
        // Every data row this write staged, counted before the files are moved
        // into the collector. It seeds the publication provenance the commit
        // action later refines into the committed snapshot's `total-records`.
        let staged_data_rows = files
            .iter()
            .filter(|file| file.content == DataContentType::Data)
            .fold(0u64, |total, file| total.saturating_add(file.record_count));
        // Branch `i`'s replacement artifacts are the ones its own writers
        // staged, so the record is keyed by the write target ordinal the
        // fragment carried and by nothing else.
        let cow_update_rewrite = cow_update_rewrite_set(
            handle,
            &validated
                .iter()
                .map(|entry| entry.ordinal)
                .zip(files.iter().cloned())
                .collect::<Vec<_>>(),
        )?;
        collector.inject_written_files(files);

        let snapshot_properties = session_snapshot_properties(handle, staged_data_rows)?;

        let binding = self
            .runtime
            .resources()
            .planning_binding()
            .for_request(context.clone());
        let access = binding.resolve_access(metadata.location())?;
        let fs = access.operator();
        let cleanup_access = access.clone();
        let cleanup_path_mapper = Some(Arc::new(move |path: &str| {
            cleanup_access
                .bind_location(path, novarocks_fs::FileIdentity::new(path, 0, None))
                .map(|file| file.operator_relative_path().to_string())
                .unwrap_or_else(|_| path.to_string())
        }) as crate::commit::CleanupPathMapper);
        let catalog = self.runtime.novarocks_catalog().vendored_client();
        let input = RunInput {
            collector,
            catalog,
            table: table.clone(),
            fs,
            file_io: table.file_io().clone(),
            cleanup_path_mapper,
            cow_update_rewrite,
            selected_rewrite: selected_rewrite_files(handle),
            // A partition replacement is a change to the table itself, and the
            // one commit that carries it has to be the one that publishes the
            // rows written under the new spec. `main` is where a managed
            // publication's default partitioning lives, so the replacement's
            // commit targets it exactly.
            target_ref: match handle.repartition() {
                Some(_) => "main".to_string(),
                None => facts.target_ref().to_string(),
            },
            snapshot_properties,
            atomic_partition_replacement: handle
                .repartition()
                .map(|prepared| {
                    crate::commit::run::AtomicPartitionReplacement::try_new(
                        prepared.metadata_updates().to_vec(),
                    )
                })
                .transpose()
                .map_err(invalid)?,
        };
        // The runtime bridge wraps the commit, so a bridge failure says nothing
        // about whether the catalog request went out. Calling it uncommitted
        // would authorize deleting files a committed snapshot may reference, so
        // it is deliberately an unknown outcome carrying real evidence.
        let bridge_collector = Arc::clone(&input.collector);
        let result = match self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { run_iceberg_commit(input).await })
        {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(error)) => Err(error),
            Err(bridge) => Err(CommitServiceError::unknown(
                format!("Iceberg commit runtime bridge: {bridge}"),
                crate::commit::service::RecoveryEvidence::from_collector(&bridge_collector),
            )),
        };
        match result {
            Ok(outcome) => {
                // The commit is proven. Everything below only *describes* it,
                // so a failure here degrades the finalization and never the
                // verdict: calling a published snapshot uncommitted would
                // authorize deleting files it already references.
                let (resulting_row_count, finalization) =
                    match self.publication_row_count(handle, outcome.new_snapshot_id, context) {
                        Ok(rows) => (rows, ExternalMutationFinalization::Complete),
                        Err(error) => (
                            None,
                            ExternalMutationFinalization::Failed(failure(
                                ConnectorMutationFailureKind::Internal,
                                error.message().to_string(),
                            )),
                        ),
                    };
                let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                    outcome.new_snapshot_id,
                    resulting_row_count,
                    handle
                        .repartition()
                        .map(|prepared| prepared.committed().clone()),
                )
                .map_err(invalid)?;
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization,
                })
            }
            Err(CommitServiceError::InvalidInput { message }) => Err(invalid(message)),
            Err(CommitServiceError::KnownUncommitted { message, cleanup }) => {
                Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: failure(
                        ConnectorMutationFailureKind::Conflict,
                        format!(
                            "{message}; staged cleanup attempted={}, errors={}",
                            cleanup.attempted, cleanup.error_count
                        ),
                    ),
                })
            }
            Err(CommitServiceError::Unknown { message, evidence }) => {
                Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: failure(ConnectorMutationFailureKind::Unavailable, message),
                    evidence: self.encode_evidence(handle, &evidence)?,
                })
            }
            Err(CommitServiceError::FinalizeFailedKnownCommitted {
                outcome,
                finalize_error,
                evidence,
            }) => match outcome {
                Some(committed) => {
                    let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                        committed.new_snapshot_id,
                        None,
                        None,
                    )
                    .map_err(invalid)?;
                    Ok(ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        receipt,
                        finalization: ExternalMutationFinalization::Failed(failure(
                            ConnectorMutationFailureKind::Internal,
                            finalize_error,
                        )),
                    })
                }
                None => Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: failure(ConnectorMutationFailureKind::Internal, finalize_error),
                    evidence: self.encode_evidence(handle, &evidence)?,
                }),
            },
        }
    }

    fn dispatch_eager_commit(
        &self,
        handle: &IcebergCommitHandle,
        validated: &[ValidatedFragment<'_>],
        statistics: Vec<novarocks_spi::connector::StatisticsArtifactDraft>,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let facts = handle.table();
        let session_abort = Arc::new(crate::commit::abort::AbortLog::new());
        for entry in validated {
            session_abort.record_data_file(entry.fragment.path().to_string());
        }
        let binding = self
            .runtime
            .resources()
            .planning_binding()
            .for_request(context.clone());
        let cleanup_access = binding.resolve_access(facts.table_location())?;
        let cleanup_fs = cleanup_access.operator();
        let cleanup_mapper: crate::commit::CleanupPathMapper = Arc::new(move |path: &str| {
            cleanup_access
                .bind_location(path, novarocks_fs::FileIdentity::new(path, 0, None))
                .map(|file| file.operator_relative_path().to_string())
                .unwrap_or_else(|_| path.to_string())
        });
        let cleanup_session = || {
            cleanup_eager_logs(
                self.runtime.as_ref(),
                cleanup_fs.clone(),
                Arc::clone(&cleanup_mapper),
                vec![Arc::clone(&session_abort)],
            );
        };
        let initial = match self.runtime.load_table_for_request(
            facts.namespace(),
            facts.table_name(),
            context,
        ) {
            Ok(table) => table.into_table(),
            Err(error) => {
                cleanup_session();
                return Err(unavailable(error.to_string()));
            }
        };
        if initial.metadata().uuid().to_string() != facts.table_uuid()
            || initial.metadata().current_schema_id() != facts.schema_id()
        {
            cleanup_session();
            return Err(invalid(
                "Iceberg write target no longer matches its sealed table generation/schema",
            ));
        }
        let observed_head = crate::ref_snapshot::resolve_branch_head_snapshot_id(
            initial.metadata(),
            facts.target_ref(),
        )
        .map_err(invalid)?;
        if handle.repartition().is_some()
            && (observed_head != facts.base_snapshot_id()
                || initial.metadata().default_partition_spec_id()
                    != facts.default_partition_spec_id())
        {
            cleanup_session();
            return Ok(ExternalMutationOutcome::KnownUncommitted {
                failure: failure(
                    ConnectorMutationFailureKind::Conflict,
                    "atomic partition replacement cannot rebase its frozen metadata updates",
                ),
            });
        }
        let commit_metadata = handle.repartition().map_or(initial.metadata(), |prepared| {
            prepared.prospective_metadata()
        });
        let files = match validated
            .iter()
            .map(|entry| written_file_from_fragment(entry.fragment, commit_metadata))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(files) => files,
            Err(error) => {
                cleanup_session();
                return Err(error);
            }
        };
        let staged_data_rows = files
            .iter()
            .filter(|file| file.content == DataContentType::Data)
            .fold(0u64, |total, file| total.saturating_add(file.record_count));
        let reusable = ReusableEagerWriteInputs::new(files, statistics);
        let snapshot_properties = match session_snapshot_properties(handle, staged_data_rows) {
            Ok(properties) => properties,
            Err(error) => {
                cleanup_session();
                return Err(error);
            }
        };
        let table_ident =
            match crate::iceberg::TableIdent::from_strs([facts.namespace(), facts.table_name()]) {
                Ok(ident) => ident,
                Err(error) => {
                    cleanup_session();
                    return Err(invalid(error.to_string()));
                }
            };
        let expected_uuid = initial.metadata().uuid();
        let expected_uuid_rendered = expected_uuid.to_string();
        let mut current = initial;
        const MAX_ATTEMPTS: usize = 3;
        for attempt in 0..MAX_ATTEMPTS {
            if let Err(error) = validate_context(context) {
                cleanup_session();
                return Err(error);
            }
            if attempt > 0 {
                self.runtime
                    .control_state()
                    .invalidate_table_cache(facts.namespace(), facts.table_name());
                current = match self.runtime.load_table_for_request(
                    facts.namespace(),
                    facts.table_name(),
                    context,
                ) {
                    Ok(table) => table.into_table(),
                    Err(error) => {
                        cleanup_session();
                        return Err(unavailable(error.to_string()));
                    }
                };
                if current.metadata().uuid() != expected_uuid
                    || current.metadata().current_schema_id() != facts.schema_id()
                {
                    cleanup_session();
                    return Ok(ExternalMutationOutcome::KnownUncommitted {
                        failure: failure(
                            ConnectorMutationFailureKind::Conflict,
                            "Iceberg write conflict retry resolved a different table generation or schema",
                        ),
                    });
                }
            }

            let attempt_metadata = handle.repartition().map_or(current.metadata(), |prepared| {
                prepared.prospective_metadata()
            });
            let attempt_partition_spec = if handle.repartition().is_some() {
                attempt_metadata.default_partition_spec().clone()
            } else {
                match current
                    .metadata()
                    .partition_spec_by_id(facts.default_partition_spec_id())
                    .cloned()
                {
                    Some(spec) => spec,
                    None => {
                        cleanup_session();
                        return Err(invalid(
                            "Iceberg write conflict retry lost the sealed partition spec",
                        ));
                    }
                }
            };
            let attempt_base_snapshot = match crate::ref_snapshot::resolve_branch_head_snapshot_id(
                current.metadata(),
                facts.target_ref(),
            ) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    cleanup_session();
                    return Err(invalid(error));
                }
            };
            let collector = Arc::new(
                IcebergCommitCollector::new(
                    handle.commit_op_kind(),
                    table_ident.clone(),
                    attempt_base_snapshot,
                    current.metadata().last_sequence_number(),
                    attempt_metadata.current_schema().clone(),
                    attempt_partition_spec,
                    handle.staging_dir(),
                )
                .with_table_metadata(attempt_metadata.clone()),
            );
            reusable.install_files(&collector);
            let commit_uuid = uuid::Uuid::new_v4();
            collector.set_manifest_cleanup_token(commit_uuid.to_string());
            let abort_handle = Arc::clone(&collector.abort_log);
            let vendored = self.runtime.novarocks_catalog().vendored_client();
            let provider_catalog = Arc::clone(self.runtime.novarocks_catalog());
            let table = current.clone();
            let file_io = table.file_io().clone();
            let target_ref = facts.target_ref().to_string();
            let snapshot_properties = snapshot_properties.clone();
            let current_artifacts = reusable.statistics_for_attempt();
            let initial_updates = handle
                .repartition()
                .map_or_else(Vec::new, |prepared| prepared.metadata_updates().to_vec());
            let mut identity = handle.session_id().to_bytes();
            identity[15] ^= attempt as u8;
            let target =
                crate::catalog::CatalogTableName::new(facts.namespace(), facts.table_name());
            let target_ref_for_request = Arc::<str>::from(facts.target_ref());
            let expected_uuid_string = Arc::<str>::from(expected_uuid_rendered.as_str());
            let marker = Some((
                Arc::<str>::from(ICEBERG_WRITE_SESSION_MARKER_PROPERTY),
                Arc::<str>::from(handle.session_id().to_string()),
            ));
            let operation = handle.commit_op_kind();
            let bridge_collector = Arc::clone(&collector);
            let attempt_result = self
                .runtime
                .resources()
                .catalog_runtime()
                .block_on(async move {
                    let ctx = crate::commit::action::CommitCtx {
                        collector: &collector,
                        table: &table,
                        catalog: vendored.as_ref(),
                        file_io: &file_io,
                        commit_uuid,
                        abort_handle,
                        target_ref: &target_ref,
                        snapshot_properties: &snapshot_properties,
                    };
                    let (mut transaction, data_outcome) = match operation {
                        CommitOpKind::FastAppend => {
                            crate::commit::fast_append::stage_eager_fast_append(ctx).await?
                        }
                        CommitOpKind::Overwrite => {
                            crate::commit::overwrite::stage_eager_overwrite(ctx, initial_updates)
                                .await?
                        }
                        _ => unreachable!("eager write path only accepts append/overwrite"),
                    };
                    let staged_metadata = transaction.staged_table().metadata().clone();
                    let sequence_number = staged_metadata.last_sequence_number();
                    let merged = artifacts_for_staged_snapshot(
                        &table,
                        &staged_metadata,
                        &target_ref,
                        operation,
                        current_artifacts,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                    if !merged.is_empty() {
                        let path = crate::stats_assembler::puffin_path_for_statistics_operation(
                            &staged_metadata,
                            data_outcome.new_snapshot_id,
                            identity,
                        );
                        collector.abort_log.record_manifest(path.clone());
                        let statistics_file = crate::stats_assembler::write_puffin_artifacts(
                            &file_io,
                            &path,
                            data_outcome.new_snapshot_id,
                            sequence_number,
                            &merged,
                        )
                        .await?
                        .ok_or_else(|| {
                            "non-empty write statistics produced no Puffin file".to_string()
                        })?;
                        transaction = transaction
                            .update_statistics()
                            .set_statistics(statistics_file)
                            .apply(transaction)
                            .await
                            .map_err(|error| {
                                format!("stage Iceberg write SetStatistics: {error}")
                            })?;
                    }
                    let mut staged = transaction.into_table_commit();
                    staged.add_requirement(crate::iceberg::TableRequirement::UuidMatch {
                        uuid: expected_uuid,
                    });
                    let request = TransactionRequest {
                        identity: TransactionIdentity::new("write-attempt", identity),
                        target,
                        target_ref: target_ref_for_request,
                        base_snapshot_id: crate::ref_snapshot::resolve_branch_head_snapshot_id(
                            table.metadata(),
                            &target_ref,
                        )
                        .map_err(|error| error.to_string())?,
                        expected_table_uuid: Some(expected_uuid_string),
                        marker,
                    };
                    let mut frontier = match provider_catalog.new_transaction(request).await {
                        CatalogTransactionStart::Ready(frontier) => frontier,
                        CatalogTransactionStart::KnownUncommitted { failure } => {
                            return Ok((
                                CatalogOutcome::KnownUncommitted { failure },
                                data_outcome,
                                collector,
                            ));
                        }
                        CatalogTransactionStart::Unsupported(error) => {
                            return Err(error.to_string());
                        }
                        CatalogTransactionStart::CommitUnknown { failure, evidence } => {
                            return Ok((
                                CatalogOutcome::CommitUnknown { failure, evidence },
                                data_outcome,
                                collector,
                            ));
                        }
                    };
                    frontier.stage(staged).map_err(|error| error.to_string())?;
                    let outcome = frontier.commit().await;
                    Ok((outcome, data_outcome, collector))
                });
            let (catalog_outcome, data_outcome, collector) = match attempt_result {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => {
                    cleanup_eager_logs(
                        self.runtime.as_ref(),
                        cleanup_fs.clone(),
                        Arc::clone(&cleanup_mapper),
                        vec![
                            Arc::clone(&bridge_collector.abort_log),
                            Arc::clone(&session_abort),
                        ],
                    );
                    return Ok(ExternalMutationOutcome::KnownUncommitted {
                        failure: failure(
                            ConnectorMutationFailureKind::InvalidRequest,
                            format!("Iceberg eager staging failed before dispatch: {error}"),
                        ),
                    });
                }
                Err(bridge) => {
                    let evidence =
                        crate::commit::service::RecoveryEvidence::from_collector(&bridge_collector);
                    return Ok(ExternalMutationOutcome::CommitUnknown {
                        failure: failure(
                            ConnectorMutationFailureKind::Unavailable,
                            format!("Iceberg eager commit runtime bridge: {bridge}"),
                        ),
                        evidence: self.encode_evidence(handle, &evidence)?,
                    });
                }
            };
            let retry_conflict = permits_fresh_eager_attempt(
                &catalog_outcome,
                attempt,
                MAX_ATTEMPTS,
                handle.repartition().is_some(),
            );
            match catalog_outcome {
                CatalogOutcome::KnownCommitted {
                    effect,
                    receipt: proof,
                    ..
                } => {
                    collector.mark_committed();
                    let proof_finalization = eager_proof_finalization(
                        &proof,
                        data_outcome.new_snapshot_id,
                        &expected_uuid_rendered,
                    );
                    let (resulting_row_count, finalization) =
                        if let Some(finalization) = proof_finalization {
                            (None, finalization)
                        } else {
                            match self.publication_row_count(
                                handle,
                                data_outcome.new_snapshot_id,
                                context,
                            ) {
                                Ok(rows) => (rows, ExternalMutationFinalization::Complete),
                                Err(error) => (
                                    None,
                                    ExternalMutationFinalization::Failed(failure(
                                        ConnectorMutationFailureKind::Internal,
                                        error.message().to_string(),
                                    )),
                                ),
                            }
                        };
                    let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                        data_outcome.new_snapshot_id,
                        resulting_row_count,
                        handle
                            .repartition()
                            .map(|prepared| prepared.committed().clone()),
                    )
                    .map_err(invalid)?;
                    return Ok(ExternalMutationOutcome::KnownCommitted {
                        effect,
                        receipt,
                        finalization,
                    });
                }
                CatalogOutcome::KnownUncommitted { .. } if retry_conflict => {
                    cleanup_eager_logs(
                        self.runtime.as_ref(),
                        cleanup_fs.clone(),
                        Arc::clone(&cleanup_mapper),
                        vec![Arc::clone(&collector.abort_log)],
                    );
                    if let Err(error) =
                        eager_conflict_backoff(self.runtime.as_ref(), context, attempt + 1)
                    {
                        cleanup_eager_logs(
                            self.runtime.as_ref(),
                            cleanup_fs.clone(),
                            Arc::clone(&cleanup_mapper),
                            vec![Arc::clone(&session_abort)],
                        );
                        return Err(error);
                    }
                }
                CatalogOutcome::KnownUncommitted { failure } => {
                    cleanup_eager_logs(
                        self.runtime.as_ref(),
                        cleanup_fs.clone(),
                        Arc::clone(&cleanup_mapper),
                        vec![Arc::clone(&collector.abort_log), Arc::clone(&session_abort)],
                    );
                    return Ok(ExternalMutationOutcome::KnownUncommitted { failure });
                }
                CatalogOutcome::CommitUnknown { failure, .. } => {
                    let evidence =
                        crate::commit::service::RecoveryEvidence::from_collector(&collector);
                    return Ok(ExternalMutationOutcome::CommitUnknown {
                        failure,
                        evidence: self.encode_evidence(handle, &evidence)?,
                    });
                }
                CatalogOutcome::Unsupported(error) => {
                    cleanup_eager_logs(
                        self.runtime.as_ref(),
                        cleanup_fs.clone(),
                        Arc::clone(&cleanup_mapper),
                        vec![Arc::clone(&collector.abort_log), Arc::clone(&session_abort)],
                    );
                    return Err(invalid(error.to_string()));
                }
            }
        }
        unreachable!("bounded eager write attempt loop always returns")
    }

    /// Project the committed snapshot's row count onto a publication's receipt.
    ///
    /// Only a managed publication claims one: its caller records the published
    /// row count as part of the refresh, and no ordinary DML receipt carries a
    /// row count at all. Returning `None` for everything else is the honest
    /// answer, not a missing feature.
    ///
    /// The read has to reload. `dispatch_commit` loaded the table before the
    /// commit and the generation-local cache still holds that pre-commit view,
    /// which by construction cannot know the snapshot just created. The reload
    /// keeps the request's already-authorized storage resolver but drops the
    /// attempt's lease sink, so it cannot admit a vended-credential response
    /// after the attempt froze.
    pub(crate) fn publication_row_count(
        &self,
        handle: &IcebergCommitHandle,
        snapshot_id: i64,
        context: &ConnectorRequestContext,
    ) -> Result<Option<u64>, ConnectorError> {
        if handle.publication().is_none() {
            return Ok(None);
        }
        let facts = handle.table();
        self.runtime
            .control_state()
            .invalidate_table_cache(facts.namespace(), facts.table_name());
        let table = self
            .runtime
            .load_table_for_request(
                facts.namespace(),
                facts.table_name(),
                &context.clone().without_vended_credential_lease_sink(),
            )
            .map_err(|error| internal(error.to_string()))?
            .into_table();
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                internal("committed Iceberg snapshot is absent during managed row-count projection")
            })?;
        snapshot
            .summary()
            .additional_properties
            .get("total-records")
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|error| {
                corrupt(format!(
                    "committed Iceberg snapshot has an unreadable row count: {error}"
                ))
            })
    }

    fn encode_evidence(
        &self,
        handle: &IcebergCommitHandle,
        recovery: &crate::commit::service::RecoveryEvidence,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        encode_session_evidence(&self.descriptor, self.key.incarnation, handle, recovery)
    }

    fn decode_evidence(
        &self,
        handle: &IcebergCommitHandle,
        evidence: &ExternalMutationEvidence,
    ) -> Result<IcebergWriteSessionEvidenceV1, ConnectorError> {
        if evidence.schema_version() != ICEBERG_WRITE_SESSION_EVIDENCE_VERSION
            || evidence.descriptor() != &self.descriptor
            || evidence.incarnation() != self.key.incarnation
            || evidence.operation_kind() != ICEBERG_WRITE_SESSION_OPERATION_KIND
        {
            return Err(invalid(
                "Iceberg write session evidence does not belong to this exact generation",
            ));
        }
        if evidence.operation_id().to_bytes() != handle.session_id().to_bytes() {
            return Err(invalid(
                "Iceberg write session evidence names a different write session",
            ));
        }
        let payload: IcebergWriteSessionEvidenceV1 =
            serde_json::from_slice(evidence.provider_payload().as_ref()).map_err(|error| {
                corrupt(format!("decode Iceberg write session evidence: {error}"))
            })?;
        if payload.version != ICEBERG_WRITE_SESSION_EVIDENCE_VERSION
            || payload.session_id != handle.session_id().to_string()
        {
            return Err(corrupt(
                "Iceberg write session evidence payload disagrees with its envelope",
            ));
        }
        Ok(payload)
    }

    /// Release a session that never reached a complete prepared write set.
    ///
    /// Without a complete set there is no trustworthy cleanup manifest, so this
    /// path deliberately does not guess which staged objects exist. It reports
    /// what it knows and never claims a commit it did not observe.
    pub fn release_session(
        &self,
        handle: &IcebergCommitHandle,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        validate_context(context)?;
        release_session_state(&self.descriptor, self.key.incarnation, handle)
    }
}

/// Terminate a session whose empty prepared write set means "do nothing".
///
/// This is the provider's own decision, taken from the session's flavor and —
/// for a publication — the disposition its caller declared. It is deliberately
/// not derivable from "there were no fragments": a zero-row `INSERT` reaches
/// `finish_write` exactly the same way and still publishes an empty snapshot.
///
/// Nothing external is touched here: no snapshot is created, the target ref
/// keeps the head the session froze, and the outcome names the version the
/// target already held so a caller records an unchanged result rather than a
/// failure. It needs no catalog at all, because every fact it uses was frozen
/// at `begin_write`.
///
/// A session whose target holds no snapshot has no version to report, so it
/// fails closed there instead of claiming a commit against nothing.
pub(crate) fn settle_empty_write_without_commit(
    handle: &IcebergCommitHandle,
) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
    let Some(unchanged_snapshot_id) = handle.table().base_snapshot_id() else {
        let message = format!(
            "Iceberg {} write produced no artifact and its target holds no snapshot to report",
            handle.flavor().as_str()
        );
        handle.settle(IcebergWriteSessionState::KnownUncommitted {
            message: message.clone(),
        })?;
        return Ok(ExternalMutationOutcome::KnownUncommitted {
            failure: failure(ConnectorMutationFailureKind::Conflict, message),
        });
    };
    let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
        unchanged_snapshot_id,
        None,
        None,
    )
    .map_err(invalid)?;
    handle.settle(IcebergWriteSessionState::KnownCommitted {
        snapshot_id: unchanged_snapshot_id,
    })?;
    Ok(ExternalMutationOutcome::KnownCommitted {
        effect: ExternalMutationEffect::NoOp,
        receipt,
        finalization: ExternalMutationFinalization::Complete,
    })
}

/// The terminal verdict a release produces, decided purely from the session's
/// own state so it can be reasoned about without a catalog.
pub(crate) fn release_session_state(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    handle: &IcebergCommitHandle,
) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
    {
        match handle.state()? {
            IcebergWriteSessionState::Active => {
                handle.settle(IcebergWriteSessionState::KnownUncommitted {
                    message: "Iceberg write session was released before any external commit"
                        .to_string(),
                })?;
                Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                    cleanup: ExternalMutationFinalization::Complete,
                })
            }
            IcebergWriteSessionState::Committing => Err(unavailable(
                "Iceberg write abort cannot race an in-progress external commit",
            )),
            IcebergWriteSessionState::KnownUncommitted { .. } => {
                Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                    cleanup: ExternalMutationFinalization::Complete,
                })
            }
            // A sealed staged write never reached the catalog, so releasing it
            // is proven-uncommitted. The staged objects it wrote belong to the
            // staged target, and the publication that owns that target is what
            // deletes them; this session has no authority over them at all.
            IcebergWriteSessionState::Sealed => Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
            }),
            IcebergWriteSessionState::KnownCommitted { snapshot_id } => {
                // Abort cannot undo a proven commit.
                let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                    snapshot_id,
                    None,
                    None,
                )
                .map_err(invalid)?;
                Ok(ConnectorWriteAbortOutcome::KnownCommitted {
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            IcebergWriteSessionState::CommitUnknown {
                message,
                staging_dir,
            } => {
                // Abort cannot resolve ambiguity, and the staged objects stay
                // where reconciliation can still find them.
                Ok(ConnectorWriteAbortOutcome::CommitUnknown {
                    failure: failure(
                        ConnectorMutationFailureKind::Unavailable,
                        format!("{message}; staged files remain at {staging_dir}"),
                    ),
                    evidence: encode_session_evidence(
                        descriptor,
                        incarnation,
                        handle,
                        &crate::commit::service::RecoveryEvidence {
                            table_ident: format!(
                                "{}.{}",
                                handle.table().namespace(),
                                handle.table().table_name()
                            ),
                            op_kind: handle.commit_op_kind(),
                            base_snapshot_id: handle.table().base_snapshot_id(),
                            base_sequence_number: handle.table().base_sequence_number(),
                            staging_dir,
                            manifest_cleanup_token: None,
                        },
                    )?,
                })
            }
        }
    }
}

/// Seal one session's recovery facts into a reconciliation evidence envelope.
pub(crate) fn encode_session_evidence(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    handle: &IcebergCommitHandle,
    recovery: &crate::commit::service::RecoveryEvidence,
) -> Result<ExternalMutationEvidence, ConnectorError> {
    let payload = IcebergWriteSessionEvidenceV1 {
        version: ICEBERG_WRITE_SESSION_EVIDENCE_VERSION,
        session_id: handle.session_id().to_string(),
        table_ident: recovery.table_ident.clone(),
        target_ref: handle.table().target_ref().to_string(),
        op_kind: format!("{:?}", recovery.op_kind),
        base_snapshot_id: recovery.base_snapshot_id,
        base_sequence_number: recovery.base_sequence_number,
        staging_dir: recovery.staging_dir.clone(),
        manifest_cleanup_token: recovery.manifest_cleanup_token.clone(),
    };
    let encoded = serde_json::to_vec(&payload)
        .map_err(|error| internal(format!("encode Iceberg write session evidence: {error}")))?;
    ExternalMutationEvidence::try_new(
        ICEBERG_WRITE_SESSION_EVIDENCE_VERSION,
        descriptor.clone(),
        incarnation,
        ConnectorMutationOperationId::from_bytes(handle.session_id().to_bytes()),
        ICEBERG_WRITE_SESSION_OPERATION_KIND,
        Bytes::from(encoded),
    )
}

impl IcebergWriteSessionControl {
    /// Resolve a session whose external outcome is unknown, by read-only
    /// adjudication against the durable snapshot marker.
    ///
    /// Marker absence is *not* proof of non-commit: the session stays unknown
    /// and every staged object is left untouched.
    pub fn adjudicate_session(
        &self,
        handle: &IcebergCommitHandle,
        evidence: &ExternalMutationEvidence,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        validate_context(context)?;
        let decoded = self.decode_evidence(handle, evidence)?;
        match handle.state()? {
            IcebergWriteSessionState::KnownCommitted { snapshot_id } => {
                let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                    snapshot_id,
                    None,
                    None,
                )
                .map_err(invalid)?;
                return Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                });
            }
            IcebergWriteSessionState::KnownUncommitted { message } => {
                return Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: failure(ConnectorMutationFailureKind::Conflict, message),
                });
            }
            IcebergWriteSessionState::Active => {
                return Err(invalid(
                    "Iceberg write reconciliation requires a prior commit-unknown outcome",
                ));
            }
            // A seal performs no external commit, so its outcome was never in
            // doubt and there is nothing for reconciliation to resolve.
            IcebergWriteSessionState::Sealed => {
                return Err(invalid(
                    "Iceberg staged write session has no external commit to reconcile",
                ));
            }
            IcebergWriteSessionState::Committing => {
                return Err(unavailable(
                    "Iceberg write reconciliation cannot race an in-progress external commit",
                ));
            }
            IcebergWriteSessionState::CommitUnknown { .. } => {}
        }

        let facts = handle.table();
        let physical = self
            .runtime
            .load_table_for_request(
                facts.namespace(),
                facts.table_name(),
                &context.clone().without_vended_credential_lease_sink(),
            )
            .map_err(|error| unavailable(error.to_string()))?;
        let table = physical.into_table();
        let metadata = table.metadata();
        if metadata.uuid().to_string() != facts.table_uuid() {
            return Err(invalid(
                "Iceberg write reconciliation loaded a different table identity",
            ));
        }
        let expected = handle.session_id().to_string();
        let mut matched: Option<i64> = None;
        for snapshot in metadata.snapshots() {
            let carried = snapshot
                .summary()
                .additional_properties
                .get(ICEBERG_WRITE_SESSION_MARKER_PROPERTY);
            if carried.is_some_and(|value| *value == expected)
                && matched.replace(snapshot.snapshot_id()).is_some()
            {
                return Err(corrupt(
                    "Iceberg write session marker matches multiple snapshots",
                ));
            }
        }
        match matched {
            Some(snapshot_id) => {
                let receipt = crate::write_codec::connector_write_receipt_with_partitioning(
                    snapshot_id,
                    None,
                    None,
                )
                .map_err(invalid)?;
                handle.settle(IcebergWriteSessionState::KnownCommitted { snapshot_id })?;
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            None => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Unavailable,
                    format!(
                        "Iceberg write session marker is absent during read-only adjudication; staged files remain at {}",
                        decoded.staging_dir
                    ),
                ),
                evidence: evidence.clone(),
            }),
        }
    }
}

/// Assemble the neutral session plan from the provider's sealed targets.
///
/// A branch's routing facts travel with it; its writer recipe stays opaque. The
/// two are deliberately separate: the recipe is what a backend executes, and the
/// route facts are what SQL needs to decide which branch a row belongs to.
pub(crate) fn session_plan_from_targets(
    adapter: &IcebergWriteAdapter,
    mut handle: IcebergCommitHandle,
    targets: Vec<crate::commit::write_stack::planning::IcebergWriteTargetPlan>,
    statistics_metadata: Option<&TableMetadata>,
) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
    let statistics_enabled = statistics_metadata.is_some_and(collect_on_write_enabled);
    let statistics_eligible = matches!(
        handle.commit_op_kind(),
        CommitOpKind::FastAppend | CommitOpKind::Overwrite
    ) && handle.flavor() != IcebergWriteFlavor::StagedCreate;
    let mut expectations = BTreeMap::new();
    let plans = targets
        .into_iter()
        .map(|target| {
            let (ordinal, writer, input, route, rewrite_source) = target.into_parts();
            let statistics = write_statistics_contract(
                &writer,
                &input,
                statistics_metadata,
                statistics_enabled && statistics_eligible,
            )?;
            if !statistics.is_empty() {
                expectations.insert(
                    ordinal,
                    statistics
                        .requirements()
                        .iter()
                        .map(|requirement| requirement.artifact().clone())
                        .collect(),
                );
            }
            let plan =
                ConnectorWriteTargetPlan::new(ordinal, adapter.wrap_writer_handle(writer), input)
                    .with_statistics_contract(statistics)?;
            let plan = match route {
                Some(route) => plan.with_route(route),
                None => plan,
            };
            let plan = match rewrite_source {
                Some(source) => plan.with_rewrite_source(source),
                None => plan,
            };
            Ok(plan)
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    handle = handle.with_statistics_expectations(expectations)?;
    let commit = adapter.wrap_commit_handle(handle);
    ConnectorWriteSessionPlan::try_new(commit, plans)
}

fn collect_on_write_enabled(metadata: &TableMetadata) -> bool {
    metadata
        .properties()
        .get(crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY)
        .is_none_or(|value| !value.eq_ignore_ascii_case("false"))
}

fn write_statistics_contract(
    writer: &crate::commit::write_stack::domain::IcebergWriterHandle,
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
    metadata: Option<&TableMetadata>,
    enabled: bool,
) -> Result<WriteStatisticsContract, ConnectorError> {
    let Some(data_recipe) = writer.data() else {
        return WriteStatisticsContract::try_new(input, Vec::new());
    };
    if !enabled {
        return WriteStatisticsContract::try_new(input, Vec::new());
    }
    let metadata = metadata
        .ok_or_else(|| invalid("Iceberg collect-on-write requires authoritative table metadata"))?;
    let iceberg_schema = metadata.current_schema();
    let arrow_schema =
        crate::iceberg::arrow::schema_to_arrow_schema(iceberg_schema).map_err(|error| {
            invalid(format!(
                "convert Iceberg statistics schema to Arrow: {error}"
            ))
        })?;
    let mut requirements = Vec::new();
    for (ordinal, binding) in input.fields().into_iter().enumerate() {
        let field = binding.field();
        let Some(field_id) = resolve_statistics_field(
            iceberg_schema,
            &arrow_schema,
            field,
            data_recipe.row_lineage(),
        )?
        else {
            continue;
        };
        if !novarocks_connector_iceberg_functions::supports_theta_input_type(field.data_type()) {
            continue;
        }
        let input = StatisticsScanColumn::try_new(
            ordinal,
            Arc::<str>::from(field.name().as_str()),
            field.data_type().clone(),
            field.is_nullable(),
        )?;
        let artifact = StatisticsArtifactIdentity::try_new(
            vec![field_id],
            crate::iceberg::puffin::APACHE_DATASKETCHES_THETA_V1,
        )?;
        requirements.push(StatisticsRequiredAggregation::try_new(
            input,
            novarocks_connector_iceberg_functions::ICEBERG_THETA_AGGREGATE_NAME,
            artifact,
        )?);
    }
    WriteStatisticsContract::try_new(input, requirements)
}

fn resolve_statistics_field(
    iceberg_schema: &crate::iceberg::spec::Schema,
    arrow_schema: &arrow::datatypes::Schema,
    field: &arrow::datatypes::Field,
    row_lineage: bool,
) -> Result<Option<i32>, ConnectorError> {
    let candidates = iceberg_schema
        .as_struct()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, candidate)| candidate.name.eq_ignore_ascii_case(field.name()))
        .collect::<Vec<_>>();
    let (schema_ordinal, iceberg_field) = match candidates.as_slice() {
        [] if row_lineage
            && matches!(
                field.name().as_str(),
                crate::row_lineage_synth::ICEBERG_ROW_ID_COL
                    | crate::row_lineage_synth::ICEBERG_LAST_UPDATED_SEQ_COL
            ) =>
        {
            return Ok(None);
        }
        [] => {
            return Err(invalid(format!(
                "Iceberg statistics input column `{}` is absent from the authoritative table schema",
                field.name()
            )));
        }
        [candidate] => *candidate,
        _ => {
            return Err(invalid(format!(
                "Iceberg statistics input column `{}` is ambiguous under case-insensitive resolution",
                field.name()
            )));
        }
    };
    let expected_arrow = arrow_type_for_write_field(
        arrow_schema.field(schema_ordinal).data_type(),
        iceberg_field.field_type.as_ref(),
    );
    if field.data_type() != &expected_arrow || field.is_nullable() == iceberg_field.required {
        return Err(invalid(format!(
            "Iceberg statistics input column `{}` does not match the authoritative table field type/nullability",
            field.name()
        )));
    }
    Ok(Some(iceberg_field.id))
}

fn arrow_type_for_write_field(
    converted: &arrow::datatypes::DataType,
    iceberg: &crate::iceberg::spec::Type,
) -> arrow::datatypes::DataType {
    use crate::iceberg::spec::{PrimitiveType, Type};
    use arrow::datatypes::{DataType, TimeUnit};

    match iceberg {
        Type::Primitive(PrimitiveType::Variant) => DataType::LargeBinary,
        Type::Primitive(PrimitiveType::Binary) => DataType::Binary,
        Type::Primitive(PrimitiveType::Timestamptz) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        Type::Primitive(PrimitiveType::TimestamptzNs) => {
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        }
        _ => converted.clone(),
    }
}

impl IcebergWriteSessionControl {
    fn frozen_references_of(
        &self,
        handle: &IcebergCommitHandle,
    ) -> BTreeMap<WriteTargetOrdinal, BTreeMap<String, Vec<String>>> {
        handle.frozen_old_references()
    }

    /// Complete admission and freeze the write recipe.
    ///
    /// Everything external this method touches is a *read*: it loads the table,
    /// resolves the target ref's head, and enumerates the base snapshot's data
    /// files. It deliberately does not read a single delete artifact — that is
    /// the whole point of the NCP-6 inversion — and it performs no external
    /// mutation, so a failure here leaves nothing started.
    fn admit(
        &self,
        request: &ConnectorWriteBeginRequest,
    ) -> Result<
        (
            IcebergCommitHandle,
            Vec<crate::commit::write_stack::planning::IcebergWriteTargetPlan>,
            TableMetadata,
        ),
        ConnectorError,
    > {
        let (namespace, table_name) = request.table.rsplit_once('.').ok_or_else(|| {
            invalid("Iceberg write target must be a namespace-qualified table name")
        })?;
        // A staged target is the one target that cannot be looked up: the
        // catalog will not know it until the publication that owns it commits.
        // So its frozen facts arrive with the request, and this branch reads
        // them instead of loading. It is deliberately the only place a session
        // accepts caller-supplied metadata, and it accepts it only for the
        // flavor whose whole definition is "there is nothing to load".
        let staged = match &request.flavor {
            ConnectorWriteSessionFlavor::StagedCreate(target) => Some(
                crate::metadata::staged_target_metadata(&self.key.instance_id, target)?,
            ),
            _ => None,
        };
        let (table, metadata) = match &staged {
            Some(staged) => {
                if staged.namespace != namespace || staged.table != table_name {
                    return Err(invalid(
                        "Iceberg staged write target names a different table than its frozen facts",
                    ));
                }
                (None, staged.metadata.clone())
            }
            None => {
                let physical = self
                    .runtime
                    .load_table_for_request(namespace, table_name, &request.context)
                    .map_err(|error| unavailable(error.to_string()))?;
                let table = physical.into_table();
                let metadata = table.metadata().clone();
                (Some(table), metadata)
            }
        };
        // The session is the write's admission point, so it applies the same
        // support guards the separate preparation call applies. Without this a
        // session would admit a write this table cannot encode, and only a
        // caller that happened to also prepare would find out. A staged target
        // is exempt: it is a table this statement is creating, so there is no
        // existing schema or spec history to be unsupported.
        if staged.is_none() {
            crate::commit::validation::ensure_iceberg_write_supported_from_metadata(&metadata)
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unsupported, error))?;
        }
        let target_ref = request.target_ref.as_str();
        let base_snapshot_id = match &staged {
            // A staged target holds no snapshot, and resolving a branch head
            // against it would either invent one or fail on an absent ref.
            Some(_) => None,
            None => crate::ref_snapshot::resolve_branch_head_snapshot_id(&metadata, target_ref)
                .map_err(|error| invalid(error.to_string()))?,
        };
        if let Some(base) = &request.base {
            if staged.is_some() {
                return Err(invalid(
                    "Iceberg staged write target has no base version to compare against",
                ));
            }
            base.validate()?;
        }
        let facts = IcebergWriteTableFacts::try_new(
            metadata.uuid().to_string(),
            namespace.to_string(),
            table_name.to_string(),
            metadata.location().to_string(),
            iceberg_data_location(&metadata),
            target_ref.to_string(),
            base_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema_id(),
            metadata.default_partition_spec_id(),
            format_version_number(&metadata),
        )?;
        // A managed publication may replace the target's default partitioning
        // inside the same external commit that publishes its rows. Preparing it
        // here, before any branch is planned, is what lets the writers write
        // under the spec the commit is about to establish: a writer that used
        // the spec the table still has would stage files partitioned against a
        // spec the same commit retires.
        let repartition = match &request.flavor {
            ConnectorWriteSessionFlavor::ManagedPublication { intent, .. } => intent
                .partition_spec_replacement()
                .map(|replacement| {
                    let prepared = crate::commit::write_stack::repartition::
                        prepare_managed_repartition(
                            &metadata,
                            replacement,
                            intent.descriptor_properties(),
                        )?;
                    // The caller signed the provider's own earlier preview into
                    // the intent. Re-deriving it here and requiring equality is
                    // the optimistic-concurrency check: a table whose spec moved
                    // between preview and admission produces a different result
                    // and is refused rather than silently repartitioned another
                    // way.
                    let expected = intent.expected_committed_partitioning().ok_or_else(|| {
                        invalid(
                            "Iceberg managed partition replacement is missing its exact preview partitioning",
                        )
                    })?;
                    if prepared.committed() != expected {
                        return Err(invalid(
                            "Iceberg managed partition replacement no longer matches its exact preview partitioning",
                        ));
                    }
                    Ok(prepared)
                })
                .transpose()?,
            _ => None,
        };
        // The generation every writer is built against. Under a partition
        // replacement it is the prospective one; the session's own facts stay
        // on the generation the table currently holds, because that is what its
        // commit-time compare-and-swap has to match.
        let writer_metadata = repartition
            .as_ref()
            .map_or(&metadata, |prepared| prepared.prospective_metadata());
        let writer_facts = match &repartition {
            None => None,
            Some(_) => Some(IcebergWriteTableFacts::try_new(
                writer_metadata.uuid().to_string(),
                namespace.to_string(),
                table_name.to_string(),
                writer_metadata.location().to_string(),
                iceberg_data_location(writer_metadata),
                target_ref.to_string(),
                base_snapshot_id,
                writer_metadata.last_sequence_number(),
                writer_metadata.current_schema_id(),
                writer_metadata.default_partition_spec_id(),
                format_version_number(writer_metadata),
            )?),
        };
        let statistics_metadata = writer_metadata.clone();
        let signed = sign_input_shape(&facts, &request.input)?;
        let material = IcebergSessionMaterial {
            data_output: IcebergWriterOutput::try_new(
                crate::delete_file::IcebergFileFormat::Parquet,
                parquet::basic::Compression::SNAPPY,
                crate::commit::data_writer::parquet_row_group_size_bytes(
                    writer_metadata.properties(),
                )
                .map_err(invalid)?
                .map(|size| size as u64),
            )?,
            data_recipe: data_branch_recipe(
                writer_metadata,
                matches!(signed, ConnectorWriteInputShape::RowLineage { .. }),
            )?,
            // A delete branch's frozen old-delete references are the one piece
            // of material that costs an external read, so it is taken only when
            // the request's own shape says a delete branch exists.
            merge_targets: if session_freezes_old_deletes(&request.flavor, &signed) {
                let snapshot_id = base_snapshot_id.ok_or_else(|| {
                    invalid("Iceberg row-level write requires a frozen target snapshot")
                })?;
                let table = table.as_ref().ok_or_else(|| {
                    invalid("Iceberg row-level write requires a loaded target table")
                })?;
                self.freeze_old_delete_references(table, &metadata, snapshot_id)?
            } else {
                Vec::new()
            },
            // The match key needs the frozen Iceberg schema to turn a column
            // name into a field id, so it is resolved here rather than in the
            // pure branch planner.
            equality: match &signed {
                ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
                    Some(equality_delete_recipe(writer_metadata, equality_fields)?)
                }
                _ => None,
            },
            table: facts,
            input: signed,
        };

        let plan = match &request.flavor {
            // A staged create writes data into an empty target, which is the
            // same branch shape an append has -- one data branch. What differs
            // is where the commit happens, and that is the flavor's business,
            // not the branch planner's.
            ConnectorWriteSessionFlavor::Ordinary
            | ConnectorWriteSessionFlavor::StagedCreate(_) => {
                plan_ordinary_branches(flavor_for(request)?, &material)?
            }
            ConnectorWriteSessionFlavor::ManagedPublication { intent, shape } => {
                plan_managed_publication_branches(&material, publication_facts(intent, *shape)?)?
            }
            ConnectorWriteSessionFlavor::RowMutation => plan_row_mutation_branches(&material)?,
            ConnectorWriteSessionFlavor::DistributedRewrite(shape) => {
                let table = table.as_ref().ok_or_else(|| {
                    invalid("Iceberg distributed rewrite requires a loaded target table")
                })?;
                let groups =
                    self.freeze_rewrite_groups(table, &metadata, base_snapshot_id, *shape)?;
                plan_distributed_rewrite_branches(&material, *shape, &groups)?
            }
            ConnectorWriteSessionFlavor::CopyOnWrite(selection) => {
                let table = table.as_ref().ok_or_else(|| {
                    invalid("Iceberg copy-on-write mutation requires a loaded target table")
                })?;
                // Without a base snapshot there is nothing to rewrite, and a
                // copy-on-write mutation that silently became an append would
                // publish after-images while every before-image stayed live.
                let snapshot_id = base_snapshot_id.ok_or_else(|| {
                    invalid("Iceberg copy-on-write mutation requires a frozen base snapshot")
                })?;
                let base_version_digest = request
                    .base
                    .as_ref()
                    .map(ConnectorWriteBaseVersion::digest)
                    .ok_or_else(|| {
                        invalid("Iceberg copy-on-write mutation requires its signed base version")
                    })?;
                let recipes =
                    crate::commit::write_stack::copy_on_write::freeze_copy_on_write_branches(
                        selection,
                        crate::commit::write_stack::copy_on_write::IcebergCowFreezeInput {
                            catalog: &self.key.instance_id,
                            namespace,
                            table_name,
                            metadata: &metadata,
                            snapshot_id,
                            base_files: self.frozen_base_data_files(table, snapshot_id)?,
                            input: &material.input,
                            base_version_digest,
                            max_handle_payload_bytes: request.context.max_handle_payload_bytes(),
                        },
                    )?;
                plan_copy_on_write_branches(&material, &recipes)?
            }
        };
        let IcebergSessionFlavorPlan {
            flavor,
            publication,
            rewrite_inputs,
            copy_on_write,
            branches,
        } = plan;
        let (handle, targets) = plan_branch_session(
            IcebergWriteSessionId::new(),
            IcebergBranchSessionPlanInput {
                flavor,
                purpose: request.purpose,
                table: material.table,
                base_version_digest: request.base.as_ref().map(|base| base.digest()),
                publication,
                staged_metadata: staged.map(|staged| Arc::new(staged.metadata)),
                rewrite_inputs,
                copy_on_write,
                repartition,
                writer_table: writer_facts,
                branches,
            },
        )?;
        Ok((handle, targets, statistics_metadata))
    }

    /// Every live data file of one frozen base snapshot.
    ///
    /// This is the same read a distributed rewrite performs to cut its groups:
    /// a walk of the frozen snapshot's manifests, and nothing else. It opens no
    /// delete artifact and mutates nothing.
    fn frozen_base_data_files(
        &self,
        table: &crate::iceberg::table::Table,
        snapshot_id: i64,
    ) -> Result<Vec<crate::manifest::DataFileWithStats>, ConnectorError> {
        let owned = table.clone();
        self.runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::manifest::extract_data_files_with_stats_at(&owned, snapshot_id).await
            })
            .map_err(|error| unavailable(error.to_string()))?
            .map_err(unavailable)
    }

    /// Freeze the rewrite groups a distributed rewrite seals one branch each
    /// for.
    ///
    /// Grouping is the provider's decision and reuses the existing rewrite
    /// planner, so a group here is the same group the rewrite path already
    /// cuts — which is why the shape's selection facts travel with the session:
    /// a position-delete rewrite's group set depends on them, and a second
    /// planner run that guessed them would seal a different branch count than
    /// the frozen plan has cohorts. Everything this touches is a read of the
    /// frozen base snapshot.
    fn freeze_rewrite_groups(
        &self,
        table: &crate::iceberg::table::Table,
        metadata: &TableMetadata,
        base_snapshot_id: Option<i64>,
        shape: ConnectorDistributedRewriteShape,
    ) -> Result<Vec<IcebergFrozenRewriteBranch>, ConnectorError> {
        // Without a base snapshot there is nothing to rewrite at all, and a
        // rewrite that silently became an append would republish rows it never
        // read.
        let Some(snapshot_id) = base_snapshot_id else {
            return Ok(Vec::new());
        };
        let owned = table.clone();
        let files = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::manifest::extract_data_files_with_stats_at(&owned, snapshot_id).await
            })
            .map_err(|error| unavailable(error.to_string()))?
            .map_err(unavailable)?;
        if files.is_empty() {
            return Ok(Vec::new());
        }
        match shape {
            ConnectorDistributedRewriteShape::DataFiles { .. } => {
                let live = crate::distributed_rewrite::live_delete_file_paths_at(
                    &self.runtime,
                    table,
                    snapshot_id,
                )?;
                Ok(
                    crate::distributed_rewrite::plan_data_file_groups(files, &live)?
                        .into_iter()
                        .map(|group| IcebergFrozenRewriteBranch {
                            group,
                            delete_targets: Vec::new(),
                        })
                        .collect(),
                )
            }
            ConnectorDistributedRewriteShape::PositionDeletes {
                rewrite_all,
                min_input_files,
            } => {
                // The delete writer resolves a data file's partition and row
                // count through the merge target frozen for it, so each group's
                // data files are kept beside the group rather than re-read
                // later from a generation that may have moved.
                let by_path = files
                    .iter()
                    .map(|file| (file.path.clone(), file.clone()))
                    .collect::<BTreeMap<_, _>>();
                crate::distributed_rewrite::plan_position_delete_groups(
                    files,
                    rewrite_all,
                    min_input_files,
                )?
                .into_iter()
                .map(|group| {
                    let delete_targets = group
                        .data_files
                        .iter()
                        .map(|file| {
                            let frozen = by_path.get(&file.path).ok_or_else(|| {
                                corrupt(format!(
                                    "Iceberg rewrite group names data file {} the frozen snapshot does not hold",
                                    file.path
                                ))
                            })?;
                            // No old reference: the rewrite reads the group's
                            // deletion vectors through its own scan and
                            // republishes their positions, so the writer has
                            // nothing left to merge.
                            frozen_delete_merge_target(frozen, metadata, snapshot_id, Vec::new())
                        })
                        .collect::<Result<Vec<_>, ConnectorError>>()?;
                    Ok(IcebergFrozenRewriteBranch {
                        group,
                        delete_targets,
                    })
                })
                .collect()
            }
        }
    }

    /// Freeze exact references to every old delete artifact attached to the
    /// base snapshot's data files.
    ///
    /// This is the frontend half of D10. It records what exists; it never opens
    /// one of those artifacts.
    fn freeze_old_delete_references(
        &self,
        table: &crate::iceberg::table::Table,
        metadata: &TableMetadata,
        snapshot_id: i64,
    ) -> Result<Vec<IcebergOldDeleteMergeTarget>, ConnectorError> {
        let owned = table.clone();
        let files = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::manifest::extract_data_files_with_stats_at(&owned, snapshot_id).await
            })
            .map_err(|error| unavailable(error.to_string()))?
            .map_err(unavailable)?;
        let mut targets = Vec::with_capacity(files.len());
        for file in files {
            let references = frozen_old_delete_references(&file)?;
            targets.push(frozen_delete_merge_target(
                &file,
                metadata,
                snapshot_id,
                references,
            )?);
        }
        Ok(targets)
    }
}

/// Freeze one data file's delete-branch merge target.
///
/// The partition descriptor and row count come off the frozen manifest entry,
/// because the backend's delete writer resolves both through this target: the
/// partition names where the replacement artifact is written and how the commit
/// decodes it, and the row count bounds every position it will accept.
///
/// `references` is stated by the caller rather than derived, because whether
/// this target's old artifacts are merged or replaced is the caller's decision:
/// a row mutation supersedes them and passes them all, a rewrite republishes
/// them from its own scan and passes none.
fn frozen_delete_merge_target(
    file: &crate::manifest::DataFileWithStats,
    metadata: &TableMetadata,
    snapshot_id: i64,
    references: Vec<IcebergOldDeleteArtifactRef>,
) -> Result<IcebergOldDeleteMergeTarget, ConnectorError> {
    let partition_spec_id = file.partition_spec_id.ok_or_else(|| {
        corrupt(format!(
            "Iceberg data file {} has no frozen partition spec ID",
            file.path
        ))
    })?;
    let partition_values = file.partition_values.as_ref().ok_or_else(|| {
        corrupt(format!(
            "Iceberg data file {} has no frozen partition values",
            file.path
        ))
    })?;
    let partition_spec = metadata
        .partition_spec_by_id(partition_spec_id)
        .ok_or_else(|| {
            corrupt(format!(
                "Iceberg data file {} references unknown partition spec {partition_spec_id}",
                file.path
            ))
        })?;
    let (partition_path, null_fingerprint) =
        crate::commit::report::partition_path_from_struct(partition_values, partition_spec)
            .map_err(corrupt)?;
    let descriptor = crate::write_descriptor::encode_partition_descriptor(
        partition_values,
        partition_spec_id,
        metadata,
    )
    .map_err(|error| corrupt(error.detail_message()))?;
    let partition = IcebergArtifactPartition::try_new(
        partition_path,
        null_fingerprint,
        partition_spec_id,
        descriptor,
    )?;
    let record_count = u64::try_from(file.record_count.unwrap_or_default()).map_err(|_| {
        corrupt(format!(
            "Iceberg data file {} has a negative record count",
            file.path
        ))
    })?;
    IcebergOldDeleteMergeTarget::try_new(
        file.path.clone(),
        record_count,
        file.data_sequence_number,
        partition,
        snapshot_id,
        references,
    )
}

/// Freeze exact references to every position-delete artifact attached to one
/// data file. It records what exists; it never opens one of those artifacts.
fn frozen_old_delete_references(
    file: &crate::manifest::DataFileWithStats,
) -> Result<Vec<IcebergOldDeleteArtifactRef>, ConnectorError> {
    let partition_spec_id = file.partition_spec_id.ok_or_else(|| {
        corrupt(format!(
            "Iceberg data file {} has no frozen partition spec ID",
            file.path
        ))
    })?;
    let mut references = Vec::new();
    for delete in &file.delete_files {
        if !matches!(
            delete.file_content,
            crate::scan_model::IcebergDeleteFileContent::Position
        ) {
            continue;
        }
        let file_format = match delete.file_format {
            crate::scan_model::IcebergDeleteFileFormat::Parquet => {
                crate::delete_file::IcebergFileFormat::Parquet
            }
            crate::scan_model::IcebergDeleteFileFormat::Puffin => {
                crate::delete_file::IcebergFileFormat::Puffin
            }
        };
        let length = delete
            .length
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| {
                corrupt(format!(
                    "Iceberg delete artifact {} has no usable frozen file size",
                    delete.path
                ))
            })?;
        let content_range = match (delete.content_offset, delete.content_size_in_bytes) {
            (Some(offset), Some(size)) => Some(IcebergContentRange::try_new(offset, size)?),
            (None, None) => None,
            _ => {
                return Err(corrupt(format!(
                    "Iceberg delete artifact {} carries a partial Puffin blob range",
                    delete.path
                )));
            }
        };
        let route = IcebergStorageRoute::try_for_location(&delete.path)?;
        references.push(IcebergOldDeleteArtifactRef::try_new(
            delete.path.clone(),
            crate::delete_file::IcebergFileContent::PositionDeletes,
            file_format,
            length,
            // The Iceberg manifest carries a record count per delete file, but
            // the provider's read-model projection does not surface it, so the
            // reference is frozen without one rather than with a guessed value.
            // The backend still rejects an exclusive artifact that decodes to
            // nothing.
            None,
            content_range,
            delete.referenced_data_file.clone(),
            delete.sequence_number,
            None,
            delete.partition_spec_id.unwrap_or(partition_spec_id),
            route,
        )?);
    }
    Ok(references)
}

fn iceberg_data_location(metadata: &TableMetadata) -> String {
    metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", metadata.location().trim_end_matches('/')))
}

fn format_version_number(metadata: &TableMetadata) -> u8 {
    match metadata.format_version() {
        crate::iceberg::spec::FormatVersion::V1 => 1,
        crate::iceberg::spec::FormatVersion::V2 => 2,
        _ => 3,
    }
}

/// What a publication needs the session to know.
///
/// The boundary the publication id must respect is a *destination* boundary,
/// not a "the provider never holds it" one. The session's writer recipes, its
/// commit fragments, and every backend that executes them have no use for the
/// id, and it reaches none of them.
///
/// It does reach exactly one thing, and must: the summary of the snapshot this
/// session's own commit creates. The publication fence adjudicates a staged
/// refresh by reading `MV_PUBLICATION_ID_PROP` back off that snapshot
/// (`catalog_control::catalog_mutation` -> `snapshot_matches_publication_marker`),
/// so a commit that omitted it would publish a snapshot no publication could
/// ever claim — and would do so silently, because nothing before the fence
/// looks for the property. Converting the intent's durable facts once, here, is
/// what keeps the id off the execution path while still putting it where the
/// fence looks.
pub(crate) fn publication_facts(
    intent: &ConnectorManagedPublicationIntent,
    shape: ConnectorManagedPublicationShape,
) -> Result<IcebergManagedPublicationFacts, ConnectorError> {
    let bases = intent
        .bases()
        .iter()
        .map(provenance_base_from_staged_fact)
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid)?;
    Ok(IcebergManagedPublicationFacts::new(
        intent.technique(),
        intent.empty_input(),
        shape,
        IcebergManagedPublicationProvenance::try_new(
            intent.publication_id(),
            bases,
            intent.definition_fingerprint().to_string(),
            base64::engine::general_purpose::STANDARD
                .encode(intent.descriptor_properties().digest()),
        )?,
    ))
}

/// One publication base, in the Iceberg provenance form the snapshot records.
///
/// The neutral base object identity stays opaque to everything but this
/// conversion, which is the only place that is allowed to know it is a
/// canonical Iceberg table UUID (ADR-0097).
fn provenance_base_from_staged_fact(
    base: &novarocks_spi::connector::ConnectorStagedPublicationBaseFact,
) -> Result<crate::commit::ProvenanceBase, String> {
    let uuid = std::str::from_utf8(base.object_id.as_bytes())
        .map_err(|error| format!("Iceberg base object ID is not UTF-8: {error}"))?;
    let parsed = uuid::Uuid::parse_str(uuid)
        .map_err(|error| format!("Iceberg base object ID is not a UUID: {error}"))?;
    if parsed.to_string() != uuid {
        return Err("Iceberg base object ID is not a canonical UUID".to_string());
    }
    Ok(crate::commit::ProvenanceBase {
        table_fqn: base.table.to_string(),
        uuid: uuid.to_string(),
        from_snapshot: base.from_version,
        to_snapshot: base.to_version,
    })
}

/// Whether this session will seal a delete branch, and therefore has to freeze
/// the old delete artifacts that branch must supersede.
pub(crate) fn session_freezes_old_deletes(
    flavor: &ConnectorWriteSessionFlavor,
    signed: &ConnectorWriteInputShape,
) -> bool {
    match flavor {
        ConnectorWriteSessionFlavor::Ordinary => {
            crate::commit::write_stack::flavor::ordinary_delete_branch(signed).is_some()
        }
        // A merge-on-read mutation seals a deletion-vector branch from a
        // row-lineage input, so the delete-shaped-input test is not enough here.
        ConnectorWriteSessionFlavor::RowMutation => !matches!(
            signed,
            ConnectorWriteInputShape::Data { .. } | ConnectorWriteInputShape::EqualityDelete { .. }
        ),
        // A publication that applies a change stream seals the same delete
        // branch a DML row mutation does, so it must supersede the same old
        // artifacts. One that republishes rows wholesale seals no delete branch
        // at all and has nothing to freeze.
        ConnectorWriteSessionFlavor::ManagedPublication { shape, .. } => {
            *shape == ConnectorManagedPublicationShape::RowMutation
                && !matches!(
                    signed,
                    ConnectorWriteInputShape::Data { .. }
                        | ConnectorWriteInputShape::EqualityDelete { .. }
                )
        }
        // A staged target has no base snapshot and therefore no old delete
        // artifact to supersede: it is a table nobody has ever written to. A
        // copy-on-write mutation seals only data branches: it replaces whole
        // files rather than staging a delete beside them, so there is no
        // artifact for one to supersede either.
        //
        // A distributed rewrite seals no session-wide freeze even when its
        // branch is a delete branch. Its branches own disjoint groups, so each
        // one's merge target is frozen per branch by `freeze_rewrite_groups`;
        // and a rewrite supersedes nothing anyway -- it reads the old artifacts
        // through its own scan and its commit retires exactly them.
        ConnectorWriteSessionFlavor::StagedCreate(_)
        | ConnectorWriteSessionFlavor::DistributedRewrite(_)
        | ConnectorWriteSessionFlavor::CopyOnWrite(_) => false,
    }
}

/// Pick the flavor an *ordinary* begin request describes.
///
/// An unsupported combination fails here rather than being coerced into the
/// nearest supported one.
fn flavor_for(request: &ConnectorWriteBeginRequest) -> Result<IcebergWriteFlavor, ConnectorError> {
    use novarocks_spi::connector::{ConnectorWriteAdmissionPurpose, ConnectorWriteIntent};
    // A staged create is decided by the flavor alone. Its intent is an append
    // and its input is data, so reading intent and input would answer
    // `Append` -- correct as far as it goes, and wrong about the one thing
    // that matters: the target does not exist yet.
    if matches!(request.flavor, ConnectorWriteSessionFlavor::StagedCreate(_)) {
        return Ok(IcebergWriteFlavor::StagedCreate);
    }
    if request.purpose == ConnectorWriteAdmissionPurpose::MaterializedViewRefresh {
        return Ok(IcebergWriteFlavor::ManagedPublication);
    }
    Ok(match (request.intent, &request.input) {
        (ConnectorWriteIntent::Append, _) => IcebergWriteFlavor::Append,
        (ConnectorWriteIntent::Overwrite, _) => IcebergWriteFlavor::Overwrite,
        (ConnectorWriteIntent::PartitionOverwrite, _) => IcebergWriteFlavor::PartitionOverwrite,
        (ConnectorWriteIntent::RowDelta, ConnectorWriteInputRequest::DeletionVector { .. }) => {
            IcebergWriteFlavor::RowMutationDeletionVector
        }
        (ConnectorWriteIntent::RowDelta, ConnectorWriteInputRequest::PositionDelete { .. }) => {
            IcebergWriteFlavor::RowMutationPositionDelete
        }
        (ConnectorWriteIntent::RowDelta, ConnectorWriteInputRequest::RowLineage { .. }) => {
            IcebergWriteFlavor::RowMutationCopyOnWrite
        }
        // `ALTER TABLE ... ADD EQUALITY DELETE`. It appends delete files rather
        // than superseding a data file's deletes, so it commits as an ordinary
        // row delta, the same op a Parquet position-delete mutation uses.
        (ConnectorWriteIntent::RowDelta, ConnectorWriteInputRequest::EqualityDelete { .. }) => {
            IcebergWriteFlavor::EqualityDelete
        }
        (ConnectorWriteIntent::RowDelta, _) => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Iceberg row-delta write requires a position-delete, deletion-vector, equality-delete, or row-lineage input",
            ));
        }
    })
}

/// Sign the caller's requested fields into a provider-owned input shape.
///
/// The token is preparation-local and derived from the exact table generation,
/// so a binding minted for one session cannot be replayed into another.
fn sign_input_shape(
    facts: &IcebergWriteTableFacts,
    request: &ConnectorWriteInputRequest,
) -> Result<ConnectorWriteInputShape, ConnectorError> {
    let sign = |tag: &str, fields: &[ConnectorWriteFieldRequest]| {
        fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let mut hasher = Sha256::new();
                hasher.update(b"novarocks.iceberg.write-stack.field.v1\0");
                hasher.update(facts.table_uuid().as_bytes());
                hasher.update([0]);
                hasher.update(facts.target_ref().as_bytes());
                hasher.update([0]);
                hasher.update(tag.as_bytes());
                hasher.update([0]);
                hasher.update(index.to_be_bytes());
                hasher.update(field.field().name().as_bytes());
                hasher.update([0]);
                hasher.update(format!("{:?}", field.field().data_type()).as_bytes());
                hasher.update([u8::from(field.field().is_nullable())]);
                let token = ConnectorWriteFieldToken::from_bytes(hasher.finalize().into());
                ConnectorWriteFieldBinding::new(token, field.field().clone())
            })
            .collect::<Vec<_>>()
    };
    let shape = match request {
        ConnectorWriteInputRequest::Data { fields } => ConnectorWriteInputShape::Data {
            fields: sign("data", fields),
        },
        ConnectorWriteInputRequest::RowLineage {
            data_fields,
            row_identity_fields,
        } => ConnectorWriteInputShape::RowLineage {
            data_fields: sign("row-lineage-data", data_fields),
            row_identity_fields: sign("row-lineage-identity", row_identity_fields),
        },
        ConnectorWriteInputRequest::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputShape::PositionDelete {
            identity_fields: sign("position-delete-identity", identity_fields),
            partition_source_fields: sign("position-delete-partition", partition_source_fields),
        },
        ConnectorWriteInputRequest::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputShape::DeletionVector {
            identity_fields: sign("deletion-vector-identity", identity_fields),
            partition_source_fields: sign("deletion-vector-partition", partition_source_fields),
        },
        ConnectorWriteInputRequest::EqualityDelete { equality_fields } => {
            ConnectorWriteInputShape::EqualityDelete {
                equality_fields: sign("equality-delete", equality_fields),
            }
        }
    };
    shape.validate()?;
    Ok(shape)
}

/// Resolve the equality-delete match key against the frozen table generation.
///
/// A column name alone is not a match key: Iceberg records the *field ids* on
/// the manifest, and a reader applies the delete by id. Resolving them here,
/// against the same metadata the session froze, is what makes a renamed or
/// absent column a refusal instead of a delete that matches nothing.
fn equality_delete_recipe(
    metadata: &TableMetadata,
    fields: &[ConnectorWriteFieldBinding],
) -> Result<crate::commit::write_stack::domain::IcebergEqualityDeleteRecipe, ConnectorError> {
    use crate::commit::write_stack::domain::{
        IcebergEqualityDeleteColumnFacts, IcebergEqualityDeleteRecipe,
    };

    // A partitioned equality delete would have to decide which partition each
    // matched row lives in without ever reading one, so it stays refused rather
    // than written into an arbitrary partition.
    if !metadata.default_partition_spec().is_unpartitioned() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg equality-delete writer supports only unpartitioned tables",
        ));
    }
    let schema = metadata.current_schema();
    let columns = fields
        .iter()
        .map(|binding| {
            let field = binding.field();
            let iceberg_field = schema
                .as_struct()
                .fields()
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(field.name()))
                .ok_or_else(|| {
                    invalid(format!(
                        "Iceberg equality-delete field `{}` is absent from the frozen schema",
                        field.name()
                    ))
                })?;
            IcebergEqualityDeleteColumnFacts::try_new(
                field.name().to_string(),
                iceberg_field.id,
                format!("{:?}", field.data_type()),
                field.is_nullable(),
            )
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    IcebergEqualityDeleteRecipe::try_new(columns)
}

/// Derive the data branch's partitioning and schema recipe from the frozen
/// table metadata.
fn data_branch_recipe(
    metadata: &TableMetadata,
    row_lineage: bool,
) -> Result<IcebergDataBranchRecipe, ConnectorError> {
    let schema = metadata.current_schema();
    let spec = metadata.default_partition_spec();
    let mut sources = Vec::with_capacity(spec.fields().len());
    let mut names = Vec::with_capacity(spec.fields().len());
    let mut transforms = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            corrupt(format!(
                "Iceberg partition field {} names unknown source id {}",
                field.name, field.source_id
            ))
        })?;
        sources.push(source.name.clone());
        names.push(field.name.clone());
        transforms.push(field.transform.to_string());
    }
    IcebergDataBranchRecipe::try_new(
        Some(crate::schema_facts::iceberg_schema_def(schema)),
        sources,
        names,
        transforms,
        row_lineage,
    )
}

impl novarocks_spi::connector::write_stack::session::ConnectorWriteControl
    for IcebergWriteSessionControl
{
    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn begin_write(
        &self,
        request: ConnectorWriteBeginRequest,
    ) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
        validate_context(&request.context)?;
        let (handle, targets, statistics_metadata) = self.admit(&request)?;
        // The frozen old-delete map is derived from the same writer handles the
        // plan carries, so `finish_write` can re-derive it without a second
        // source of truth.
        let plan =
            session_plan_from_targets(&self.adapter, handle, targets, Some(&statistics_metadata))?;
        Ok(plan)
    }

    fn finish_write(
        &self,
        request: ConnectorWriteFinishRequest<'_>,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let handle = self.adapter.commit_handle(request.commit)?;
        let statistics = validate_statistics_artifacts(handle, request.statistics)?;
        let frozen = self.frozen_references_of(handle);
        // A staged create is the one flavor that finishes without committing:
        // its single external effect belongs to the publication that owns the
        // staged target, and performing one here would create a snapshot on a
        // table the catalog does not yet have.
        if handle.flavor() == IcebergWriteFlavor::StagedCreate {
            return self.seal_staged_prepared_set(
                handle,
                &request.prepared,
                &frozen,
                &request.context,
            );
        }
        self.commit_prepared_set(
            handle,
            &request.prepared,
            statistics,
            &frozen,
            &request.context,
        )
    }

    fn abort_write(
        &self,
        request: ConnectorWriteSessionAbortRequest<'_>,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        let handle = self.adapter.commit_handle(request.commit)?;
        self.release_session(handle, &request.context)
    }

    fn reconcile_write(
        &self,
        request: ConnectorWriteSessionReconcileRequest<'_>,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let handle = self.adapter.commit_handle(request.commit)?;
        self.adjudicate_session(handle, &request.evidence, &request.context)
    }
}

#[cfg(test)]
mod statistics_contract_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};

    use super::{eager_proof_finalization, resolve_statistics_field, validate_theta_properties};
    use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    fn schema(fields: Vec<Arc<NestedField>>) -> Schema {
        Schema::builder()
            .with_fields(fields)
            .build()
            .expect("schema")
    }

    #[test]
    fn ambiguous_case_insensitive_statistics_field_is_rejected() {
        let iceberg = schema(vec![
            Arc::new(NestedField::required(
                1,
                "Foo",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                2,
                "foo",
                Type::Primitive(PrimitiveType::Long),
            )),
        ]);
        let arrow = crate::iceberg::arrow::schema_to_arrow_schema(&iceberg).expect("arrow");
        assert!(
            resolve_statistics_field(
                &iceberg,
                &arrow,
                &Field::new("FOO", DataType::Int64, false),
                false,
            )
            .expect_err("ambiguous casing")
            .message()
            .contains("ambiguous")
        );
    }

    #[test]
    fn statistics_field_type_and_nullability_must_match_catalog_schema() {
        let iceberg = schema(vec![Arc::new(NestedField::required(
            1,
            "v",
            Type::Primitive(PrimitiveType::Long),
        ))]);
        let arrow = crate::iceberg::arrow::schema_to_arrow_schema(&iceberg).expect("arrow");
        for field in [
            Field::new("v", DataType::Int32, false),
            Field::new("v", DataType::Int64, true),
        ] {
            assert!(resolve_statistics_field(&iceberg, &arrow, &field, false).is_err());
        }
    }

    #[test]
    fn mismatched_commit_proof_is_a_committed_finalization_failure() {
        let wrong_snapshot =
            crate::catalog::transaction::CommitProof::applied(Some(8)).with_table_uuid("table-a");
        assert!(matches!(
            eager_proof_finalization(&wrong_snapshot, 7, "table-a"),
            Some(novarocks_spi::connector::ExternalMutationFinalization::Failed(_))
        ));
        let wrong_uuid =
            crate::catalog::transaction::CommitProof::applied(Some(7)).with_table_uuid("table-b");
        assert!(matches!(
            eager_proof_finalization(&wrong_uuid, 7, "table-a"),
            Some(novarocks_spi::connector::ExternalMutationFinalization::Failed(_))
        ));
        let exact =
            crate::catalog::transaction::CommitProof::applied(Some(7)).with_table_uuid("table-a");
        assert!(eager_proof_finalization(&exact, 7, "table-a").is_none());
    }

    #[test]
    fn supplied_ndv_property_must_match_theta_body_estimate() {
        let exact = BTreeMap::from([(crate::stats_loader::NDV_PROPERTY.to_string(), "7".into())]);
        validate_theta_properties(&exact, 7.0).expect("exact NDV");

        let mismatched =
            BTreeMap::from([(crate::stats_loader::NDV_PROPERTY.to_string(), "8".into())]);
        assert!(validate_theta_properties(&mismatched, 7.0).is_err());

        let non_numeric = BTreeMap::from([(
            crate::stats_loader::NDV_PROPERTY.to_string(),
            "seven".into(),
        )]);
        assert!(validate_theta_properties(&non_numeric, 7.0).is_err());
    }
}

#[cfg(test)]
mod eager_attempt_io_tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::catalog::CatalogTableName;
    use crate::catalog::error::CatalogCommitEvidence;
    use crate::catalog::transaction::{CatalogCommitDispatch, CommitProof, TransactionShape};
    use crate::iceberg::io::{
        FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, MemoryStorage, OutputFile,
        Storage, StorageConfig, StorageFactory,
    };
    use crate::iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use crate::iceberg::spec::{
        DataContentType, DataFileFormat, FormatVersion, NestedField, PartitionSpec, PrimitiveType,
        Schema, SortOrder, Struct, TableMetadataBuilder, Type,
    };
    use crate::iceberg::{CatalogBuilder, Error as IcebergError, ErrorKind, TableIdent};

    const DATA_PATH: &str = "memory://warehouse/db/t/data/already-written.parquet";

    #[derive(Debug, Default)]
    struct IoTrace {
        data_input_opens: AtomicUsize,
        data_exists_calls: AtomicUsize,
        data_metadata_calls: AtomicUsize,
        data_read_calls: AtomicUsize,
        data_reader_calls: AtomicUsize,
        data_read_bytes: AtomicU64,
        puffin_input_opens: AtomicUsize,
        puffin_output_opens: AtomicUsize,
        puffin_metadata_calls: AtomicUsize,
        puffin_read_calls: AtomicUsize,
        puffin_read_bytes: AtomicU64,
        puffin_write_calls: AtomicUsize,
        puffin_write_bytes: AtomicU64,
        written_paths: Mutex<Vec<String>>,
    }

    impl IoTrace {
        fn is_data(&self, path: &str) -> bool {
            path == DATA_PATH
        }

        fn is_puffin(&self, path: &str) -> bool {
            path.ends_with(".puffin")
        }

        fn observe_write(&self, path: &str) {
            self.written_paths
                .lock()
                .expect("I/O trace lock")
                .push(path.to_string());
        }

        fn assert_data_was_not_reopened(&self) {
            assert_eq!(self.data_input_opens.load(Ordering::SeqCst), 0);
            assert_eq!(self.data_exists_calls.load(Ordering::SeqCst), 0);
            assert_eq!(self.data_metadata_calls.load(Ordering::SeqCst), 0);
            assert_eq!(self.data_read_calls.load(Ordering::SeqCst), 0);
            assert_eq!(self.data_reader_calls.load(Ordering::SeqCst), 0);
            assert_eq!(self.data_read_bytes.load(Ordering::SeqCst), 0);
        }

        fn attempt_derivative_counts(&self) -> (usize, usize) {
            let paths = self.written_paths.lock().expect("I/O trace lock");
            (
                paths.iter().filter(|path| path.ends_with(".avro")).count(),
                paths
                    .iter()
                    .filter(|path| path.ends_with(".puffin"))
                    .count(),
            )
        }

        fn publication_observation(&self) -> serde_json::Value {
            serde_json::json!({
                "record": "iceberg_publication_io",
                "schema_version": 1,
                "scope": "one_eager_collect_on_write_attempt",
                "observations": {
                    "puffin_write_calls": observed(
                        self.puffin_write_calls.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.FileWrite.write",
                    ),
                    "puffin_write_bytes": observed(
                        self.puffin_write_bytes.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.FileWrite.write.bytes",
                    ),
                    "puffin_read_calls": observed(
                        self.puffin_read_calls.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.FileRead.read",
                    ),
                    "puffin_read_bytes": observed(
                        self.puffin_read_bytes.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.FileRead.read.bytes",
                    ),
                    "puffin_metadata_calls": observed(
                        self.puffin_metadata_calls.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.Storage.metadata",
                    ),
                    "puffin_input_opens": observed(
                        self.puffin_input_opens.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.Storage.new_input",
                    ),
                    "puffin_output_opens": observed(
                        self.puffin_output_opens.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.Storage.new_output",
                    ),
                    "data_input_opens": observed(
                        self.data_input_opens.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.Storage.new_input[data_file]",
                    ),
                    "data_exists_calls": observed(
                        self.data_exists_calls.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.Storage.exists[data_file]",
                    ),
                    "data_metadata_calls": observed(
                        self.data_metadata_calls.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.Storage.metadata[data_file]",
                    ),
                    "data_read_calls": observed(
                        self.data_read_calls.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.Storage.read_or_FileRead.read[data_file]",
                    ),
                    "data_reader_calls": observed(
                        self.data_reader_calls.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.Storage.reader[data_file]",
                    ),
                    "data_read_bytes": observed(
                        self.data_read_bytes.load(Ordering::SeqCst),
                        "iceberg.counting_file_io.read_bytes[data_file]",
                    ),
                },
            })
        }
    }

    fn observed(value: impl Serialize, source: &'static str) -> serde_json::Value {
        serde_json::json!({"value": value, "source": source, "observed": true})
    }

    struct CountingFileRead {
        inner: Box<dyn FileRead>,
        trace: Arc<IoTrace>,
        path: String,
    }

    #[async_trait]
    impl FileRead for CountingFileRead {
        async fn read(&self, range: std::ops::Range<u64>) -> crate::iceberg::Result<Bytes> {
            let bytes = self.inner.read(range).await?;
            if self.trace.is_data(&self.path) {
                self.trace.data_read_calls.fetch_add(1, Ordering::SeqCst);
                self.trace
                    .data_read_bytes
                    .fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
            if self.trace.is_puffin(&self.path) {
                self.trace.puffin_read_calls.fetch_add(1, Ordering::SeqCst);
                self.trace
                    .puffin_read_bytes
                    .fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
            Ok(bytes)
        }
    }

    struct CountingFileWrite {
        inner: Box<dyn FileWrite>,
        trace: Arc<IoTrace>,
        path: String,
    }

    #[async_trait]
    impl FileWrite for CountingFileWrite {
        async fn write(&mut self, bytes: Bytes) -> crate::iceberg::Result<()> {
            let len = bytes.len() as u64;
            self.inner.write(bytes).await?;
            if self.trace.is_puffin(&self.path) {
                self.trace.puffin_write_calls.fetch_add(1, Ordering::SeqCst);
                self.trace
                    .puffin_write_bytes
                    .fetch_add(len, Ordering::SeqCst);
            }
            Ok(())
        }

        async fn close(&mut self) -> crate::iceberg::Result<()> {
            self.inner.close().await
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct CountingStorage {
        inner: MemoryStorage,
        #[serde(skip, default)]
        trace: Arc<IoTrace>,
    }

    #[typetag::serde(name = "ncp8-counting-memory-storage")]
    #[async_trait]
    impl Storage for CountingStorage {
        async fn exists(&self, path: &str) -> crate::iceberg::Result<bool> {
            if self.trace.is_data(path) {
                self.trace.data_exists_calls.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.exists(path).await
        }

        async fn list_directories(&self, path: &str) -> crate::iceberg::Result<Vec<String>> {
            self.inner.list_directories(path).await
        }

        async fn metadata(&self, path: &str) -> crate::iceberg::Result<FileMetadata> {
            if self.trace.is_data(path) {
                self.trace
                    .data_metadata_calls
                    .fetch_add(1, Ordering::SeqCst);
            }
            if self.trace.is_puffin(path) {
                self.trace
                    .puffin_metadata_calls
                    .fetch_add(1, Ordering::SeqCst);
            }
            self.inner.metadata(path).await
        }

        async fn read(&self, path: &str) -> crate::iceberg::Result<Bytes> {
            let bytes = self.inner.read(path).await?;
            if self.trace.is_data(path) {
                self.trace.data_read_calls.fetch_add(1, Ordering::SeqCst);
                self.trace
                    .data_read_bytes
                    .fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
            if self.trace.is_puffin(path) {
                self.trace.puffin_read_calls.fetch_add(1, Ordering::SeqCst);
                self.trace
                    .puffin_read_bytes
                    .fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
            Ok(bytes)
        }

        async fn reader(&self, path: &str) -> crate::iceberg::Result<Box<dyn FileRead>> {
            if self.trace.is_data(path) {
                self.trace.data_reader_calls.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Box::new(CountingFileRead {
                inner: self.inner.reader(path).await?,
                trace: Arc::clone(&self.trace),
                path: path.to_string(),
            }))
        }

        async fn write(&self, path: &str, bytes: Bytes) -> crate::iceberg::Result<()> {
            let len = bytes.len() as u64;
            self.trace.observe_write(path);
            self.inner.write(path, bytes).await?;
            if self.trace.is_puffin(path) {
                self.trace.puffin_write_calls.fetch_add(1, Ordering::SeqCst);
                self.trace
                    .puffin_write_bytes
                    .fetch_add(len, Ordering::SeqCst);
            }
            Ok(())
        }

        async fn writer(&self, path: &str) -> crate::iceberg::Result<Box<dyn FileWrite>> {
            self.trace.observe_write(path);
            Ok(Box::new(CountingFileWrite {
                inner: self.inner.writer(path).await?,
                trace: Arc::clone(&self.trace),
                path: path.to_string(),
            }))
        }

        async fn delete(&self, path: &str) -> crate::iceberg::Result<()> {
            self.inner.delete(path).await
        }

        async fn delete_prefix(&self, path: &str) -> crate::iceberg::Result<()> {
            self.inner.delete_prefix(path).await
        }

        fn new_input(&self, path: &str) -> crate::iceberg::Result<InputFile> {
            if self.trace.is_data(path) {
                self.trace.data_input_opens.fetch_add(1, Ordering::SeqCst);
            }
            if self.trace.is_puffin(path) {
                self.trace.puffin_input_opens.fetch_add(1, Ordering::SeqCst);
            }
            Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
        }

        fn new_output(&self, path: &str) -> crate::iceberg::Result<OutputFile> {
            if self.trace.is_puffin(path) {
                self.trace
                    .puffin_output_opens
                    .fetch_add(1, Ordering::SeqCst);
            }
            Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct CountingStorageFactory {
        inner: MemoryStorage,
        #[serde(skip, default)]
        trace: Arc<IoTrace>,
    }

    #[typetag::serde(name = "ncp8-counting-memory-storage-factory")]
    impl StorageFactory for CountingStorageFactory {
        fn build(&self, _config: &StorageConfig) -> crate::iceberg::Result<Arc<dyn Storage>> {
            Ok(Arc::new(CountingStorage {
                inner: self.inner.clone(),
                trace: Arc::clone(&self.trace),
            }))
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum DispatchBehavior {
        Conflict,
        Commit,
        LoseResponse,
    }

    #[derive(Debug)]
    struct CountingDispatch {
        behavior: DispatchBehavior,
        dispatches: AtomicUsize,
    }

    impl CountingDispatch {
        fn new(behavior: DispatchBehavior) -> Arc<Self> {
            Arc::new(Self {
                behavior,
                dispatches: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl CatalogCommitDispatch for CountingDispatch {
        async fn dispatch_once(
            &self,
            staged: Option<crate::iceberg::TableCommit>,
        ) -> Result<CommitProof, IcebergError> {
            assert!(
                staged.is_some(),
                "an eager attempt must dispatch its staged commit"
            );
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                DispatchBehavior::Conflict => Err(IcebergError::new(
                    ErrorKind::CatalogCommitConflicts,
                    "injected OCC conflict",
                )),
                DispatchBehavior::Commit => Ok(CommitProof::applied(Some(1))),
                DispatchBehavior::LoseResponse => Err(IcebergError::new(
                    ErrorKind::Unexpected,
                    "injected lost response",
                )),
            }
        }

        async fn adjudicate(&self) -> Result<Option<CommitProof>, ConnectorError> {
            Ok(None)
        }
    }

    struct Fixture {
        table: crate::iceberg::table::Table,
        catalog: crate::iceberg::MemoryCatalog,
        reusable: ReusableEagerWriteInputs,
        original_body_address: usize,
        trace: Arc<IoTrace>,
    }

    async fn fixture() -> Fixture {
        let trace = Arc::new(IoTrace::default());
        let factory = Arc::new(CountingStorageFactory {
            inner: MemoryStorage::new(),
            trace: Arc::clone(&trace),
        });
        let file_io = FileIOBuilder::new(factory.clone()).build();
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/db/t".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = crate::iceberg::table::Table::builder()
            .identifier(TableIdent::from_strs(["db", "t"]).expect("identifier"))
            .metadata(metadata)
            .file_io(file_io)
            .build()
            .expect("table");
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(factory)
            .load(
                "counting",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory://warehouse".to_string(),
                )]),
            )
            .await
            .expect("memory catalog");
        let body = Bytes::from_static(include_bytes!(
            "../../../../../../tests/datasketches-tck/fixtures/theta/rust_quickselect_n1000_ordered_v3.sk"
        ));
        let original_body_address = body.as_ptr() as usize;
        let statistics = normalized_theta_artifact(
            StatisticsArtifactIdentity::try_new(
                vec![1],
                crate::iceberg::puffin::APACHE_DATASKETCHES_THETA_V1,
            )
            .expect("statistics identity"),
            body,
        )
        .expect("statistics artifact");
        let file = WrittenFile {
            path: DATA_PATH.to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count: 1_000,
            file_size_in_bytes: 8_192,
            split_offsets: Vec::new(),
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        };
        Fixture {
            table,
            catalog,
            reusable: ReusableEagerWriteInputs::new(vec![file], vec![statistics]),
            original_body_address,
            trace,
        }
    }

    struct StagedAttempt {
        frontier: crate::catalog::transaction::Transaction,
        snapshot_id: i64,
        manifest_paths: Vec<String>,
        puffin_path: String,
        body_address: usize,
    }

    async fn stage_attempt(
        fixture: &Fixture,
        attempt: u8,
        dispatch: Arc<CountingDispatch>,
    ) -> StagedAttempt {
        let table = fixture.table.clone();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::FastAppend,
                table.identifier().clone(),
                None,
                table.metadata().last_sequence_number(),
                table.metadata().current_schema().clone(),
                table.metadata().default_partition_spec().clone(),
                "memory://warehouse/db/t/staging".to_string(),
            )
            .with_table_metadata(table.metadata().clone()),
        );
        fixture.reusable.install_files(&collector);
        let abort_handle = Arc::clone(&collector.abort_log);
        let commit_uuid = uuid::Uuid::from_bytes([attempt; 16]);
        let properties = BTreeMap::new();
        let (mut transaction, outcome) =
            crate::commit::fast_append::stage_eager_fast_append(crate::commit::action::CommitCtx {
                collector: &collector,
                table: &table,
                catalog: &fixture.catalog,
                file_io: table.file_io(),
                commit_uuid,
                abort_handle,
                target_ref: "main",
                snapshot_properties: &properties,
            })
            .await
            .expect("eager append stage");
        let staged_metadata = transaction.staged_table().metadata().clone();
        let artifacts = artifacts_for_staged_snapshot(
            &table,
            &staged_metadata,
            "main",
            CommitOpKind::FastAppend,
            fixture.reusable.statistics_for_attempt(),
        )
        .await
        .expect("statistics for staged snapshot");
        assert_eq!(artifacts.len(), 1);
        let body_address = artifacts[0].body().as_ptr() as usize;
        let puffin_path = crate::stats_assembler::puffin_path_for_statistics_operation(
            &staged_metadata,
            outcome.new_snapshot_id,
            [attempt; 16],
        );
        collector.abort_log.record_manifest(puffin_path.clone());
        let statistics_file = crate::stats_assembler::write_puffin_artifacts(
            table.file_io(),
            &puffin_path,
            outcome.new_snapshot_id,
            staged_metadata.last_sequence_number(),
            &artifacts,
        )
        .await
        .expect("write attempt Puffin")
        .expect("non-empty statistics file");
        transaction = transaction
            .update_statistics()
            .set_statistics(statistics_file)
            .apply(transaction)
            .await
            .expect("stage SetStatistics");
        let mut commit = transaction.into_table_commit();
        commit.add_requirement(crate::iceberg::TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        });
        let mut frontier = crate::catalog::transaction::Transaction::new(
            TransactionIdentity::new("eager-I/O-test", [attempt; 16]),
            CatalogTableName::new("db", "t"),
            TransactionShape::Existing,
            CatalogCommitEvidence::for_target("db.t")
                .with_commit_uuid(format!("attempt-{attempt}")),
            dispatch,
        );
        frontier.stage(commit).expect("stage provider frontier");
        StagedAttempt {
            frontier,
            snapshot_id: outcome.new_snapshot_id,
            manifest_paths: outcome.written_manifest_paths,
            puffin_path,
            body_address,
        }
    }

    #[tokio::test]
    async fn conflict_fresh_attempt_reuses_data_and_computed_body_without_data_io() {
        let fixture = fixture().await;
        let conflict = CountingDispatch::new(DispatchBehavior::Conflict);
        let mut first = stage_attempt(&fixture, 1, Arc::clone(&conflict)).await;
        let first_outcome = first.frontier.commit().await;
        assert!(permits_fresh_eager_attempt(&first_outcome, 0, 3, false));
        assert_eq!(conflict.dispatches.load(Ordering::SeqCst), 1);

        let committed = CountingDispatch::new(DispatchBehavior::Commit);
        let mut fresh = stage_attempt(&fixture, 2, Arc::clone(&committed)).await;
        let fresh_outcome = fresh.frontier.commit().await;
        assert!(matches!(
            fresh_outcome,
            CatalogOutcome::KnownCommitted { .. }
        ));
        assert_eq!(committed.dispatches.load(Ordering::SeqCst), 1);

        assert_ne!(first.snapshot_id, fresh.snapshot_id);
        assert_ne!(first.manifest_paths, fresh.manifest_paths);
        assert_ne!(first.puffin_path, fresh.puffin_path);
        assert_eq!(first.body_address, fixture.original_body_address);
        assert_eq!(fresh.body_address, fixture.original_body_address);
        fixture.trace.assert_data_was_not_reopened();
        let (manifest_writes, puffin_writes) = fixture.trace.attempt_derivative_counts();
        assert!(
            manifest_writes >= 4,
            "each attempt must rebuild manifest metadata"
        );
        assert_eq!(
            puffin_writes, 2,
            "each attempt must rebuild one Puffin file"
        );
    }

    #[tokio::test]
    async fn ac32_publication_observability_report_has_real_puffin_io_and_no_data_reread() {
        let fixture = fixture().await;
        let committed = CountingDispatch::new(DispatchBehavior::Commit);
        let mut attempt = stage_attempt(&fixture, 8, Arc::clone(&committed)).await;
        let outcome = attempt.frontier.commit().await;
        assert!(matches!(outcome, CatalogOutcome::KnownCommitted { .. }));
        assert_eq!(committed.dispatches.load(Ordering::SeqCst), 1);
        fixture.trace.assert_data_was_not_reopened();
        assert!(
            fixture.trace.puffin_write_calls.load(Ordering::SeqCst) > 0,
            "the publication observer must see actual Puffin writes"
        );
        assert!(
            fixture.trace.puffin_write_bytes.load(Ordering::SeqCst) > 0,
            "the publication observer must see actual Puffin write bytes"
        );
        assert!(
            fixture.trace.puffin_read_calls.load(Ordering::SeqCst) > 0,
            "the publication observer must see the Puffin sizing read"
        );
        assert!(
            fixture.trace.puffin_read_bytes.load(Ordering::SeqCst) > 0,
            "the publication observer must see actual Puffin read bytes"
        );
        println!(
            "NCP8_AC32_ICEBERG_IO={}",
            fixture.trace.publication_observation()
        );
    }

    #[tokio::test]
    async fn commit_unknown_never_stages_or_dispatches_a_fresh_attempt() {
        let fixture = fixture().await;
        let lost = CountingDispatch::new(DispatchBehavior::LoseResponse);
        let mut attempt = stage_attempt(&fixture, 7, Arc::clone(&lost)).await;
        let before_unknown = fixture.trace.attempt_derivative_counts();
        let outcome = attempt.frontier.commit().await;
        assert!(matches!(outcome, CatalogOutcome::CommitUnknown { .. }));
        assert!(!permits_fresh_eager_attempt(&outcome, 0, 3, false));

        let repeated = attempt.frontier.commit().await;
        assert!(matches!(repeated, CatalogOutcome::CommitUnknown { .. }));
        assert_eq!(lost.dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.trace.attempt_derivative_counts(), before_unknown);
        fixture.trace.assert_data_was_not_reopened();
    }
}
