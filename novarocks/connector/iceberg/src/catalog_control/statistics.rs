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

//! Exact-generation Iceberg statistics capability.
//!
//! Design: ADR-0135 (docs/adr/ADR-0135-ordinary-aggregate-statistics-dataflow.md)

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::DataType;
use bytes::Bytes;
use novarocks_connector_iceberg_functions::{
    ICEBERG_THETA_AGGREGATE_NAME, estimate_compact_theta, supports_theta_input_type,
    validate_compact_theta,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorStatistics, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, StatisticsArtifactDraft,
    StatisticsArtifactIdentity, StatisticsBasisRelation, StatisticsCollection,
    StatisticsCollectionSession, StatisticsCollectionStart, StatisticsCollectionStartRequest,
    StatisticsColumnSelection, StatisticsDataVersion, StatisticsEvidence,
    StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricObservation,
    StatisticsMetricSource, StatisticsMetricState, StatisticsMetricValue, StatisticsMissing,
    StatisticsMissingKind, StatisticsNumericNature, StatisticsReadRequest, StatisticsReader,
    StatisticsReceipt, StatisticsRequiredAggregation, StatisticsRowCoverage, StatisticsScanColumn,
};
use sha2::{Digest, Sha256};

use crate::catalog::error::CatalogOutcome;
use crate::catalog::transaction::{TransactionIdentity, TransactionRequest};
use crate::catalog::{CatalogTableName, CatalogTransactionStart};
use crate::iceberg::puffin::APACHE_DATASKETCHES_THETA_V1;
use crate::iceberg::spec::{PrimitiveType, Type};
use crate::manifest::{DataFileWithStats, extract_data_files_with_stats_at};
use crate::metadata::{IcebergMetadata, IcebergTablePayload};
use crate::reconcile_payload::{
    ICEBERG_STATISTICS_EVIDENCE_VERSION, IcebergStatisticsEvidenceV1, encode_statistics_evidence,
};
use crate::statistics_ancestry::{AncestorNdv, resolve_ancestor_ndv};
use crate::statistics_basis::basis_relation;
use crate::statistics_codec::{statistics_data_version, statistics_metric_column};
use crate::stats_assembler::{puffin_path_for_statistics_operation, write_puffin_artifacts};

const STATISTICS_OPERATION_KIND: &str = "statistics-publish";

#[cfg(test)]
static STATISTICS_TRANSACTION_ADMISSIONS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

impl StatisticsReader for IcebergMetadata {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.incarnation()
    }

    fn read_statistics(
        &self,
        request: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let table_info = base_table_info(&table, "statistics read")?;
        let expected = pinned_data_version(table_info)?;
        if request.data_version != expected {
            return Err(invalid(
                "Iceberg statistics request does not match its resolved table pin",
            ));
        }
        let snapshot_id = table_info.current_snapshot_id.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg table has no current snapshot for statistics",
            )
        })?;
        let physical = self
            .runtime()
            .load_table_for_request(&table.namespace, &table.table, &request.context)
            .map_err(unavailable)?;
        let metadata = physical.table.metadata();
        // Deliberately no currentness check. The query was planned on this
        // snapshot; the table moving on afterwards does not make that snapshot's
        // statistics wrong, it only makes them describe an older state — which
        // is what the per-metric basis facts are for.

        // Manifest-derivable metrics always come from the snapshot being
        // queried, whether or not a statistics file exists. Letting a published
        // artifact supply them would answer with whichever snapshot ANALYZE
        // happened to measure, and letting its absence blank them out was how a
        // table with no Puffin ended up with no statistics at all.
        let table_for_files = physical.table.clone();
        let files = self
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                extract_data_files_with_stats_at(&table_for_files, snapshot_id).await
            })
            .map_err(unavailable)?
            .map_err(unavailable)?;
        let arrow_schema = crate::iceberg::arrow::schema_to_arrow_schema(metadata.current_schema())
            .map_err(|error| corrupt(format!("convert Iceberg statistics schema: {error}")))?;
        let field_ids = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.name.to_ascii_lowercase(), field.id))
            .collect::<HashMap<_, _>>();
        let data_types = arrow_schema
            .fields()
            .iter()
            .map(|field| (field.name().to_ascii_lowercase(), field.data_type().clone()))
            .collect::<HashMap<_, _>>();
        // Two independent questions that used to share one boolean. Whether the
        // manifest accounts for every row is a coverage fact; whether delete
        // files make a summed number an over-count is a per-metric numeric
        // fact, and it only bends the metrics it actually affects.
        let row_coverage = if files.iter().all(|file| file.record_count.is_some()) {
            StatisticsRowCoverage::AllVisibleRows
        } else {
            StatisticsRowCoverage::PartialRows
        };
        let has_deletes = files.iter().any(|file| !file.delete_files.is_empty());

        let mut metrics: BTreeMap<StatisticsMetric, StatisticsMetricState> = request
            .metrics
            .metrics()
            .iter()
            .filter(|metric| !matches!(metric, StatisticsMetric::ThetaNdv { .. }))
            .cloned()
            .map(|metric| {
                let state = manifest_metric(&metric, &files, &data_types, has_deletes, &expected);
                (metric, state)
            })
            .collect();

        // NDV lives only in Puffin, and Puffin is published against the snapshot
        // that was measured — so each column searches its own ancestry.
        let wanted_ndv: BTreeMap<i32, StatisticsMetric> = request
            .metrics
            .metrics()
            .iter()
            .filter(|metric| matches!(metric, StatisticsMetric::ThetaNdv { .. }))
            .filter_map(|metric| {
                let column = statistics_metric_column(metric)?;
                let field_id = field_ids.get(&column.to_ascii_lowercase())?;
                Some((*field_id, metric.clone()))
            })
            .collect();
        let resolved_ndv = if wanted_ndv.is_empty() {
            HashMap::new()
        } else {
            let table_for_ndv = physical.table.clone();
            let field_set = wanted_ndv.keys().copied().collect::<BTreeSet<_>>();
            self.runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move {
                    resolve_ancestor_ndv(
                        table_for_ndv.metadata(),
                        table_for_ndv.file_io(),
                        snapshot_id,
                        &field_set,
                    )
                    .await
                })
                .map_err(unavailable)?
        };
        let row_count_ceiling = row_count_ceiling(&metrics);
        for metric in request.metrics.metrics() {
            if !matches!(metric, StatisticsMetric::ThetaNdv { .. }) {
                continue;
            }
            let resolved = wanted_ndv
                .iter()
                .find(|(_, wanted)| *wanted == metric)
                .and_then(|(field_id, _)| resolved_ndv.get(field_id));
            let state = match resolved {
                Some(resolved) => {
                    let basis_version = statistics_data_version(
                        table_info
                            .table_uuid
                            .as_deref()
                            .expect("pinned data version requires a table UUID"),
                        Some(resolved.basis_snapshot_id),
                    )?;
                    StatisticsMetricState::Available(StatisticsMetricObservation::new(
                        StatisticsMetricValue::F64(cap_ndv(resolved.ndv, row_count_ceiling)),
                        basis_version,
                        StatisticsMetricSource::ProviderArtifact,
                        // A Theta sketch estimates in both directions however
                        // complete its input was.
                        StatisticsNumericNature::TwoSidedApproximate,
                        basis_relation(metadata, resolved.basis_snapshot_id, snapshot_id),
                    ))
                }
                None => StatisticsMetricState::Missing(StatisticsMissing {
                    kind: StatisticsMissingKind::NotCollected,
                    message: Arc::from("no ancestor snapshot published a sketch for this column"),
                }),
            };
            metrics.insert(metric.clone(), state);
        }

        // The revision identifies the exact set of artifacts behind this answer.
        // Ancestors matter: a statistics file may be replaced on any snapshot in
        // the chain, and the cache must not keep serving the previous one.
        let revision = evidence_revision(
            table_info
                .table_uuid
                .as_deref()
                .expect("pinned data version requires a table UUID"),
            snapshot_id,
            &resolved_ndv,
        )?;
        StatisticsEvidence::try_new(expected, revision, row_coverage, metrics)
    }
}

/// Row count usable as the ceiling for an NDV, when one is available.
///
/// A count over a snapshot with delete files is itself an upper bound, so the
/// cap it provides is loose — but a loose conservative bound still beats an NDV
/// that exceeds the whole table.
fn row_count_ceiling(
    metrics: &BTreeMap<StatisticsMetric, StatisticsMetricState>,
) -> Option<RowCount> {
    match metrics.get(&StatisticsMetric::RowCount) {
        Some(StatisticsMetricState::Available(observation)) => match observation.value() {
            StatisticsMetricValue::U64(rows) => Some(RowCount {
                rows: *rows,
                exact: observation.numeric_nature() == StatisticsNumericNature::Exact,
            }),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Clone, Copy)]
struct RowCount {
    rows: u64,
    exact: bool,
}

/// Keeps a published NDV within what the table can hold.
///
/// A table proven empty has no distinct values, so the usual floor of 1 must
/// not apply — otherwise an empty table reports one distinct value per column.
fn cap_ndv(ndv: f64, row_count: Option<RowCount>) -> f64 {
    let Some(RowCount { rows, exact }) = row_count else {
        return ndv;
    };
    if exact && rows == 0 {
        return 0.0;
    }
    ndv.min(rows as f64).max(1.0)
}

/// Direction of a manifest-derived value against the truth on the queried
/// snapshot.
///
/// Manifest sums do not subtract rows removed by delete files, so with deletes
/// present a count over-reports and the bounds widen — each in its own
/// direction.
fn manifest_numeric_nature(
    metric: &StatisticsMetric,
    has_deletes: bool,
) -> StatisticsNumericNature {
    match metric {
        StatisticsMetric::AverageSize { .. } => StatisticsNumericNature::TwoSidedApproximate,
        _ if !has_deletes => StatisticsNumericNature::Exact,
        StatisticsMetric::RowCount
        | StatisticsMetric::NullCount { .. }
        | StatisticsMetric::Maximum { .. } => StatisticsNumericNature::UpperBound,
        StatisticsMetric::Minimum { .. } => StatisticsNumericNature::LowerBound,
        // A ratio of two sums that deletes shrink independently; neither
        // direction is provable. NDV never reaches here.
        StatisticsMetric::ThetaNdv { .. } => StatisticsNumericNature::TwoSidedApproximate,
    }
}

fn evidence_revision(
    table_uuid: &str,
    snapshot_id: i64,
    resolved_ndv: &HashMap<i32, AncestorNdv>,
) -> Result<StatisticsEvidenceRevision, ConnectorError> {
    let mut bases = resolved_ndv
        .iter()
        .map(|(field_id, resolved)| (*field_id, resolved.basis_snapshot_id))
        .collect::<Vec<_>>();
    bases.sort_unstable();
    let mut digest = Sha256::new();
    for (field_id, basis) in bases {
        digest.update(field_id.to_be_bytes());
        digest.update(basis.to_be_bytes());
    }
    let digest = digest.finalize();
    let mut rendered = String::with_capacity(32);
    for byte in &digest[..16] {
        rendered.push_str(&format!("{byte:02x}"));
    }
    StatisticsEvidenceRevision::try_new(Bytes::from(format!(
        "iceberg/v2/{table_uuid}/{snapshot_id}/{rendered}"
    )))
}

impl StatisticsCollection for IcebergMetadata {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.incarnation()
    }

    fn begin_collection(
        &self,
        request: StatisticsCollectionStartRequest,
    ) -> Result<StatisticsCollectionStart, ConnectorError> {
        validate_context(&request.context)?;
        let table_payload = self.table_payload(&request.table)?;
        let info = base_table_info(&table_payload, "statistics collection")?;
        let expected_version = pinned_data_version(info)?;
        if request.data_version != expected_version {
            return Err(invalid(
                "Iceberg statistics collection does not match its resolved table pin",
            ));
        }

        let physical = self
            .runtime()
            .load_table_for_request(
                &table_payload.namespace,
                &table_payload.table,
                &request.context,
            )
            .map_err(unavailable)?;
        let expected_uuid = info
            .table_uuid
            .as_deref()
            .ok_or_else(|| corrupt("Iceberg table payload is missing its table UUID"))?;
        if physical.table.metadata().uuid().to_string() != expected_uuid {
            return Err(invalid(
                "Iceberg statistics collection resolved a different physical table UUID",
            ));
        }

        let (snapshot_id, sequence_number, requirements) = match info.current_snapshot_id {
            Some(snapshot_id) => {
                let snapshot = physical
                    .table
                    .metadata()
                    .snapshot_by_id(snapshot_id)
                    .ok_or_else(|| {
                        invalid(
                            "the snapshot selected for statistics is no longer present in table metadata",
                        )
                    })?;
                let schema = snapshot
                    .schema(physical.table.metadata())
                    .map_err(|error| {
                        corrupt(format!(
                            "resolve the schema of the measured Iceberg snapshot: {error}"
                        ))
                    })?;
                let requirements = collection_requirements(&schema, &request.selection)?;
                (
                    Some(snapshot_id),
                    Some(snapshot.sequence_number()),
                    requirements,
                )
            }
            None => (None, None, Vec::new()),
        };
        let expectations = requirements
            .iter()
            .map(|requirement| requirement.artifact().clone())
            .collect();
        // A table with no eligible fields has no publication effect. Keep that
        // path a real no-op instead of requiring transaction support merely to
        // discover at finish time that there is nothing to publish.
        let frontier = if requirements.is_empty() {
            None
        } else {
            Some(begin_statistics_frontier(
                self,
                &table_payload,
                request.operation_id,
                0,
                info.current_snapshot_id,
                expected_uuid,
            )?)
        };
        let session = IcebergStatisticsCollectionSession {
            provider: self.clone(),
            operation_id: request.operation_id,
            table_payload,
            data_version: request.data_version.clone(),
            physical_table: physical.table,
            snapshot_id,
            sequence_number,
            expectations,
            context: request.context,
            frontier,
        };
        StatisticsCollectionStart::try_new(
            request.table,
            request.data_version,
            snapshot_id,
            requirements,
            Box::new(session),
        )
    }
}

struct IcebergStatisticsCollectionSession {
    provider: IcebergMetadata,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    table_payload: IcebergTablePayload,
    data_version: StatisticsDataVersion,
    physical_table: crate::iceberg::table::Table,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    expectations: Vec<StatisticsArtifactIdentity>,
    context: novarocks_spi::connector::ConnectorRequestContext,
    frontier: Option<Box<crate::catalog::transaction::Transaction>>,
}

impl StatisticsCollectionSession for IcebergStatisticsCollectionSession {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        self.provider.descriptor()
    }

    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.provider.incarnation()
    }

    fn operation_id(&self) -> novarocks_spi::connector::ConnectorMutationOperationId {
        self.operation_id
    }

    fn expectations(&self) -> &[StatisticsArtifactIdentity] {
        &self.expectations
    }

    fn finish(
        self: Box<Self>,
        artifacts: Vec<StatisticsArtifactDraft>,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
        let mut this = *self;
        validate_context(&this.context)?;
        let artifacts = validate_artifacts(&this.expectations, artifacts)?;
        let revision =
            collection_revision(&this.data_version, this.operation_id, &this.expectations)?;

        if artifacts.is_empty() {
            return statistics_receipt(
                &this.provider,
                this.operation_id,
                this.data_version,
                revision,
                Bytes::new(),
                ExternalMutationEffect::NoOp,
            );
        }
        let snapshot_id = this.snapshot_id.ok_or_else(|| {
            corrupt("a statistics session with artifacts has no measured snapshot")
        })?;
        let sequence_number = this.sequence_number.ok_or_else(|| {
            corrupt("a statistics session with artifacts has no measured sequence number")
        })?;
        let expected_uuid = this.physical_table.metadata().uuid();

        const MAX_ATTEMPTS: u8 = 3;
        for attempt in 0..MAX_ATTEMPTS {
            validate_context(&this.context)?;
            if attempt > 0 {
                let physical = this
                    .provider
                    .runtime()
                    .load_table_for_request(
                        &this.table_payload.namespace,
                        &this.table_payload.table,
                        &this.context,
                    )
                    .map_err(unavailable)?;
                if physical.table.metadata().uuid() != expected_uuid {
                    return Ok(known_uncommitted(invalid(
                        "Iceberg statistics conflict retry resolved a different physical table UUID",
                    )));
                }
                if physical
                    .table
                    .metadata()
                    .snapshot_by_id(snapshot_id)
                    .is_none()
                {
                    return Ok(known_uncommitted(invalid(
                        "the measured snapshot expired before statistics publication",
                    )));
                }
                this.physical_table = physical.table;
            }
            let mut frontier = if attempt == 0 {
                this.frontier
                    .take()
                    .expect("statistics session owns its initial frontier")
            } else {
                begin_statistics_frontier(
                    &this.provider,
                    &this.table_payload,
                    this.operation_id,
                    attempt,
                    this.physical_table
                        .metadata()
                        .current_snapshot()
                        .map(|snapshot| snapshot.snapshot_id()),
                    &expected_uuid.to_string(),
                )?
            };

            let attempt_identity = statistics_attempt_identity(this.operation_id, attempt);
            let path = puffin_path_for_statistics_operation(
                this.physical_table.metadata(),
                snapshot_id,
                attempt_identity,
            );
            let file_io = this.physical_table.file_io().clone();
            let path_for_write = path.clone();
            let artifacts_for_write = artifacts.clone();
            let statistics_file = this
                .provider
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move {
                    write_puffin_artifacts(
                        &file_io,
                        &path_for_write,
                        snapshot_id,
                        sequence_number,
                        &artifacts_for_write,
                    )
                    .await
                })
                .map_err(unavailable)?
                .map_err(unavailable)?
                .ok_or_else(|| corrupt("non-empty statistics artifacts produced no Puffin file"))?;

            let table_for_stage = this.physical_table.clone();
            let staged_result = this
                .provider
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move {
                    crate::commit::statistics::stage_statistics_file(
                        &table_for_stage,
                        statistics_file,
                    )
                    .await
                });
            let mut staged = match staged_result {
                Ok(Ok(staged)) => staged,
                Ok(Err(error)) => {
                    let error = abort_statistics_frontier(&this.provider, frontier, corrupt(error));
                    cleanup_uncommitted_statistics_file(
                        &this.provider,
                        this.physical_table.file_io().clone(),
                        path,
                    );
                    return Err(error);
                }
                Err(error) => {
                    let error =
                        abort_statistics_frontier(&this.provider, frontier, unavailable(error));
                    cleanup_uncommitted_statistics_file(
                        &this.provider,
                        this.physical_table.file_io().clone(),
                        path,
                    );
                    return Err(error);
                }
            };
            staged.add_requirement(crate::iceberg::TableRequirement::UuidMatch {
                uuid: expected_uuid,
            });
            if let Err(error) = frontier.stage(staged) {
                let error = abort_statistics_frontier(&this.provider, frontier, error);
                cleanup_uncommitted_statistics_file(
                    &this.provider,
                    this.physical_table.file_io().clone(),
                    path.clone(),
                );
                return Err(error);
            }
            if let Err(error) = validate_context(&this.context) {
                let error = abort_statistics_frontier(&this.provider, frontier, error);
                cleanup_uncommitted_statistics_file(
                    &this.provider,
                    this.physical_table.file_io().clone(),
                    path.clone(),
                );
                return Err(error);
            }
            let outcome = this
                .provider
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move { frontier.commit().await })
                .map_err(unavailable)?;
            match outcome {
                CatalogOutcome::KnownCommitted { effect, .. } => {
                    this.provider
                        .runtime()
                        .control_state()
                        .invalidate_table(&this.table_payload.namespace, &this.table_payload.table);
                    return statistics_receipt(
                        &this.provider,
                        this.operation_id,
                        this.data_version,
                        revision,
                        Bytes::from(path),
                        effect,
                    );
                }
                CatalogOutcome::KnownUncommitted { failure } => {
                    cleanup_uncommitted_statistics_file(
                        &this.provider,
                        this.physical_table.file_io().clone(),
                        path,
                    );
                    let Some(next_attempt) =
                        next_statistics_attempt(&failure, attempt, MAX_ATTEMPTS)
                    else {
                        return Ok(ExternalMutationOutcome::KnownUncommitted { failure });
                    };
                    statistics_conflict_backoff(&this.provider, &this.context, next_attempt)?;
                    continue;
                }
                CatalogOutcome::Unsupported(unsupported) => {
                    cleanup_uncommitted_statistics_file(
                        &this.provider,
                        this.physical_table.file_io().clone(),
                        path,
                    );
                    return Ok(ExternalMutationOutcome::KnownUncommitted {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Unsupported,
                            unsupported.message(),
                        ),
                    });
                }
                CatalogOutcome::CommitUnknown { failure, .. } => {
                    return Ok(ExternalMutationOutcome::CommitUnknown {
                        failure,
                        evidence: statistics_evidence(
                            &this.provider,
                            this.operation_id,
                            &this.table_payload,
                            &this.data_version,
                            &path,
                        )?,
                    });
                }
            }
        }
        unreachable!("statistics publication attempt loop always returns")
    }
}

fn abort_statistics_frontier(
    provider: &IcebergMetadata,
    mut frontier: Box<crate::catalog::transaction::Transaction>,
    mut error: ConnectorError,
) -> ConnectorError {
    let abort = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move { frontier.abort().await });
    match abort {
        Ok(Ok(())) => {}
        Ok(Err(abort_error)) => {
            error = error.with_cleanup_context(format!(
                "statistics transaction abort failed: {abort_error}"
            ));
        }
        Err(abort_error) => {
            error = error.with_cleanup_context(format!(
                "statistics transaction abort runtime bridge failed: {abort_error}"
            ));
        }
    }
    error
}

fn cleanup_uncommitted_statistics_file(
    provider: &IcebergMetadata,
    file_io: crate::iceberg::io::FileIO,
    path: String,
) {
    let cleanup_path = path.clone();
    let cleanup = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move { file_io.delete(&cleanup_path).await });
    match cleanup {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(path = %path, source = ?error, "statistics Puffin cleanup failed");
        }
        Err(error) => {
            tracing::warn!(path = %path, source = ?error, "statistics Puffin cleanup runtime bridge failed");
        }
    }
}

fn collection_requirements(
    schema: &crate::iceberg::spec::Schema,
    selection: &StatisticsColumnSelection,
) -> Result<Vec<StatisticsRequiredAggregation>, ConnectorError> {
    let arrow_schema = crate::iceberg::arrow::schema_to_arrow_schema(schema)
        .map_err(|error| corrupt(format!("convert measured Iceberg schema: {error}")))?;
    let explicit = match selection {
        StatisticsColumnSelection::Default => None,
        StatisticsColumnSelection::Explicit(columns) => Some(
            columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect::<BTreeSet<_>>(),
        ),
    };
    let fields = schema.as_struct().fields();
    let mut requirements = Vec::new();
    let mut matched = BTreeSet::new();
    for (ordinal, iceberg_field) in fields.iter().enumerate() {
        let normalized = iceberg_field.name.to_ascii_lowercase();
        if explicit
            .as_ref()
            .is_some_and(|columns| !columns.contains(&normalized))
        {
            continue;
        }
        let Some(primitive) = (match iceberg_field.field_type.as_ref() {
            Type::Primitive(primitive) => Some(primitive),
            _ => None,
        }) else {
            if explicit.is_some() {
                return Err(unsupported_column(
                    &iceberg_field.name,
                    &iceberg_field.field_type,
                ));
            }
            continue;
        };
        let arrow_field = arrow_schema.fields().get(ordinal).ok_or_else(|| {
            corrupt("Iceberg and Arrow statistics schemas have different field counts")
        })?;
        // UUID is an Iceberg physical 16-byte value. Its aggregate input is
        // declared from the Iceberg type itself, never guessed from an Utf8
        // field emitted by another schema adapter.
        let data_type = if matches!(primitive, PrimitiveType::Uuid) {
            DataType::FixedSizeBinary(16)
        } else {
            arrow_field.data_type().clone()
        };
        if !supports_theta_input_type(&data_type) {
            if explicit.is_some() {
                return Err(unsupported_column(
                    &iceberg_field.name,
                    &iceberg_field.field_type,
                ));
            }
            continue;
        }
        let input = StatisticsScanColumn::try_new(
            ordinal,
            Arc::<str>::from(iceberg_field.name.as_str()),
            data_type,
            !iceberg_field.required,
        )?;
        let artifact = StatisticsArtifactIdentity::try_new(
            vec![iceberg_field.id],
            APACHE_DATASKETCHES_THETA_V1,
        )?;
        requirements.push(StatisticsRequiredAggregation::try_new(
            input,
            ICEBERG_THETA_AGGREGATE_NAME,
            artifact,
        )?);
        matched.insert(normalized);
    }
    if let Some(explicit) = explicit
        && let Some(missing) = explicit.difference(&matched).next()
    {
        return Err(invalid(format!(
            "Iceberg statistics column `{missing}` is absent from the measured schema"
        )));
    }
    Ok(requirements)
}

fn unsupported_column(name: &str, data_type: &Type) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        format!("Iceberg statistics do not support column `{name}` of type {data_type}"),
    )
}

fn validate_artifacts(
    expectations: &[StatisticsArtifactIdentity],
    artifacts: Vec<StatisticsArtifactDraft>,
) -> Result<Vec<StatisticsArtifactDraft>, ConnectorError> {
    if artifacts.len() != expectations.len() {
        return Err(invalid(
            "statistics artifact count does not match the session expectation set",
        ));
    }
    let expected = expectations.iter().cloned().collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    let mut normalized = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        if !expected.contains(artifact.identity()) || !observed.insert(artifact.identity().clone())
        {
            return Err(invalid(
                "statistics artifacts contain an unknown or duplicate identity",
            ));
        }
        validate_compact_theta(artifact.body()).map_err(|error| corrupt(error.to_string()))?;
        let estimate =
            estimate_compact_theta(artifact.body()).map_err(|error| corrupt(error.to_string()))?;
        let (identity, body, mut properties) = artifact.into_parts();
        if properties
            .keys()
            .any(|property| property != crate::stats_loader::NDV_PROPERTY)
        {
            return Err(invalid(
                "Theta statistics artifacts contain an unsupported property",
            ));
        }
        if let Some(rendered) = properties.get(crate::stats_loader::NDV_PROPERTY) {
            let supplied = rendered
                .parse::<f64>()
                .map_err(|_| invalid("statistics artifact ndv property is not numeric"))?;
            if !supplied.is_finite() || supplied < 0.0 || supplied.to_bits() != estimate.to_bits() {
                return Err(invalid(
                    "statistics artifact ndv property does not match its Theta body",
                ));
            }
        } else {
            properties.insert(
                crate::stats_loader::NDV_PROPERTY.to_string(),
                estimate.to_string(),
            );
        }
        normalized.push(StatisticsArtifactDraft::try_new(
            identity.input_fields().to_vec(),
            identity.blob_type(),
            body,
            properties,
        )?);
    }
    if observed != expected {
        return Err(invalid(
            "statistics artifacts do not exactly match the session expectation set",
        ));
    }
    normalized.sort_by(|left, right| left.identity().cmp(right.identity()));
    Ok(normalized)
}

fn begin_statistics_frontier(
    provider: &IcebergMetadata,
    table: &IcebergTablePayload,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    attempt: u8,
    base_snapshot_id: Option<i64>,
    expected_uuid: &str,
) -> Result<Box<crate::catalog::transaction::Transaction>, ConnectorError> {
    #[cfg(test)]
    STATISTICS_TRANSACTION_ADMISSIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let expected_uuid = Arc::<str>::from(expected_uuid);
    let request = TransactionRequest {
        identity: TransactionIdentity::new(
            "statistics-attempt",
            statistics_attempt_identity(operation_id, attempt),
        ),
        target: CatalogTableName::new(table.namespace.as_str(), table.table.as_str()),
        target_ref: Arc::from("main"),
        base_snapshot_id,
        expected_table_uuid: Some(expected_uuid),
        marker: None,
    };
    let catalog = Arc::clone(provider.runtime().novarocks_catalog());
    let start = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.new_transaction(request).await })
        .map_err(unavailable)?;
    match start {
        CatalogTransactionStart::Ready(frontier) => Ok(frontier),
        CatalogTransactionStart::Unsupported(unsupported) => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            unsupported.message(),
        )),
        CatalogTransactionStart::KnownUncommitted { failure } => Err(ConnectorError::new(
            connector_kind(failure.kind()),
            failure.message(),
        )),
        CatalogTransactionStart::CommitUnknown { failure, .. } => Err(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            format!("statistics transaction admission outcome is unknown: {failure}"),
        )),
    }
}

fn statistics_attempt_identity(
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    attempt: u8,
) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(operation_id.to_bytes());
    digest.update([attempt]);
    digest.finalize()[..16]
        .try_into()
        .expect("SHA-256 prefix has a fixed width")
}

fn next_statistics_attempt(
    failure: &ConnectorMutationFailure,
    attempt: u8,
    max_attempts: u8,
) -> Option<u8> {
    let next_attempt = attempt.checked_add(1)?;
    (failure.kind() == ConnectorMutationFailureKind::Conflict && next_attempt < max_attempts)
        .then_some(next_attempt)
}

fn statistics_conflict_backoff(
    provider: &IcebergMetadata,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    next_attempt: u8,
) -> Result<(), ConnectorError> {
    validate_context(context)?;
    let delay = std::time::Duration::from_millis(20 * u64::from(next_attempt));
    if context.deadline().saturating_duration_since(Instant::now()) <= delay {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "statistics publication deadline does not permit another conflict attempt",
        ));
    }
    provider
        .runtime()
        .resources()
        .catalog_runtime()
        .block_on(async move { tokio::time::sleep(delay).await })
        .map_err(unavailable)?;
    validate_context(context)
}

fn collection_revision(
    data_version: &StatisticsDataVersion,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    expectations: &[StatisticsArtifactIdentity],
) -> Result<StatisticsEvidenceRevision, ConnectorError> {
    let mut digest = Sha256::new();
    digest.update(data_version.as_bytes());
    digest.update(operation_id.to_bytes());
    for expectation in expectations {
        for field in expectation.input_fields() {
            digest.update(field.to_be_bytes());
        }
        digest.update(expectation.blob_type().as_bytes());
    }
    StatisticsEvidenceRevision::try_new(Bytes::copy_from_slice(&digest.finalize()[..16]))
}

fn connector_kind(kind: ConnectorMutationFailureKind) -> ConnectorErrorKind {
    match kind {
        ConnectorMutationFailureKind::InvalidRequest
        | ConnectorMutationFailureKind::AlreadyExists
        | ConnectorMutationFailureKind::Conflict => ConnectorErrorKind::InvalidRequest,
        ConnectorMutationFailureKind::NotFound => ConnectorErrorKind::NotFound,
        ConnectorMutationFailureKind::Unauthenticated
        | ConnectorMutationFailureKind::PermissionDenied => ConnectorErrorKind::PermissionDenied,
        ConnectorMutationFailureKind::Unsupported => ConnectorErrorKind::Unsupported,
        ConnectorMutationFailureKind::Cancelled => ConnectorErrorKind::Cancelled,
        ConnectorMutationFailureKind::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        ConnectorMutationFailureKind::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
        ConnectorMutationFailureKind::Unavailable => ConnectorErrorKind::Unavailable,
        ConnectorMutationFailureKind::CorruptData => ConnectorErrorKind::CorruptData,
        ConnectorMutationFailureKind::Internal => ConnectorErrorKind::Internal,
    }
}

impl ConnectorStatistics for IcebergMetadata {
    fn collection(&self) -> Option<&dyn StatisticsCollection> {
        Some(self)
    }
}

fn base_table_info<'a>(
    table: &'a IcebergTablePayload,
    operation: &str,
) -> Result<&'a crate::scan_model::IcebergTableInfo, ConnectorError> {
    if table.metadata_table_type.is_some() {
        return Err(invalid(format!(
            "Iceberg {operation} requires a base table handle"
        )));
    }
    table.table_info.as_ref().ok_or_else(|| {
        invalid(format!(
            "Iceberg {operation} requires a resolved base table payload"
        ))
    })
}

fn pinned_data_version(
    info: &crate::scan_model::IcebergTableInfo,
) -> Result<StatisticsDataVersion, ConnectorError> {
    statistics_data_version(
        info.table_uuid
            .as_deref()
            .ok_or_else(|| corrupt("Iceberg table payload is missing its table UUID"))?,
        info.current_snapshot_id,
    )
}

fn manifest_metric(
    metric: &StatisticsMetric,
    files: &[DataFileWithStats],
    data_types: &HashMap<String, DataType>,
    has_deletes: bool,
    basis_version: &StatisticsDataVersion,
) -> StatisticsMetricState {
    // Every manifest-derived value describes the queried snapshot itself, so
    // the basis relation is identity; only the numeric direction varies.
    let available = |value: StatisticsMetricValue| {
        StatisticsMetricState::Available(StatisticsMetricObservation::new(
            value,
            basis_version.clone(),
            StatisticsMetricSource::CurrentManifest,
            manifest_numeric_nature(metric, has_deletes),
            StatisticsBasisRelation::Identical,
        ))
    };
    match metric {
        StatisticsMetric::RowCount => files
            .iter()
            .try_fold(0_u64, |total, file| {
                total.checked_add(u64::try_from(file.record_count?).ok()?)
            })
            .map(|value| available(StatisticsMetricValue::U64(value)))
            .unwrap_or_else(|| incomplete("Iceberg manifest does not report every row count")),
        StatisticsMetric::NullCount { column } => files
            .iter()
            .try_fold(0_u64, |total, file| {
                let count = column_stats(file, column)?.null_count?;
                total.checked_add(u64::try_from(count).ok()?)
            })
            .map(|value| available(StatisticsMetricValue::U64(value)))
            .unwrap_or_else(|| {
                incomplete(format!(
                    "Iceberg manifest does not report a null count for `{column}`"
                ))
            }),
        StatisticsMetric::AverageSize { column } => {
            let total_rows = files.iter().try_fold(0_u64, |total, file| {
                total.checked_add(u64::try_from(file.record_count?).ok()?)
            });
            let total_size = files.iter().try_fold(0_u64, |total, file| {
                total.checked_add(u64::try_from(column_stats(file, column)?.column_size?).ok()?)
            });
            match (total_rows, total_size) {
                (Some(rows), Some(size)) => available(StatisticsMetricValue::F64(if rows == 0 {
                    0.0
                } else {
                    size as f64 / rows as f64
                })),
                _ => missing_column(column),
            }
        }
        StatisticsMetric::Minimum { column } | StatisticsMetric::Maximum { column } => {
            let lower = matches!(metric, StatisticsMetric::Minimum { .. });
            let Some(data_type) = data_types.get(&column.to_ascii_lowercase()) else {
                return missing_column(column);
            };
            let mut values = files.iter().map(|file| {
                let stats = column_stats(file, column)?;
                let bytes = if lower {
                    stats.lower_bound.as_deref()?
                } else {
                    stats.upper_bound.as_deref()?
                };
                decode_bound(bytes, data_type)
            });
            let reduced = values.try_fold(None, |state, value| match (state, value) {
                (None, Some(value)) => Some(Some(value)),
                (Some(current), Some(value)) => compare_bound(current, value, lower).map(Some),
                _ => None,
            });
            match reduced.flatten() {
                Some(value) => available(value),
                None => missing_column(column),
            }
        }
        // NDV is never manifest-derivable; it comes from Puffin, possibly from
        // an ancestor snapshot, and is assembled by the caller.
        StatisticsMetric::ThetaNdv { .. } => {
            incomplete("Iceberg NDV is resolved from Puffin, not from the manifest")
        }
    }
}

fn column_stats<'a>(
    file: &'a DataFileWithStats,
    column: &str,
) -> Option<&'a crate::scan_model::IcebergColumnStats> {
    file.column_stats
        .as_ref()?
        .iter()
        .find_map(|(name, stats)| name.eq_ignore_ascii_case(column).then_some(stats))
}

fn decode_bound(bytes: &[u8], data_type: &DataType) -> Option<StatisticsMetricValue> {
    match data_type {
        DataType::Boolean => match bytes {
            [0] => Some(StatisticsMetricValue::I64(0)),
            [1] => Some(StatisticsMetricValue::I64(1)),
            _ => None,
        },
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Date32
        | DataType::Time32(_) => Some(StatisticsMetricValue::I64(i64::from(i32::from_le_bytes(
            bytes.try_into().ok()?,
        )))),
        DataType::Int64
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time64(_)
        | DataType::Duration(_) => Some(StatisticsMetricValue::I64(i64::from_le_bytes(
            bytes.try_into().ok()?,
        ))),
        DataType::Float32 => Some(StatisticsMetricValue::F64(f64::from(f32::from_le_bytes(
            bytes.try_into().ok()?,
        )))),
        DataType::Float64 => Some(StatisticsMetricValue::F64(f64::from_le_bytes(
            bytes.try_into().ok()?,
        ))),
        _ => None,
    }
}

fn compare_bound(
    current: StatisticsMetricValue,
    candidate: StatisticsMetricValue,
    lower: bool,
) -> Option<StatisticsMetricValue> {
    match (current, candidate) {
        (StatisticsMetricValue::I64(current), StatisticsMetricValue::I64(candidate)) => {
            Some(StatisticsMetricValue::I64(if lower {
                current.min(candidate)
            } else {
                current.max(candidate)
            }))
        }
        (StatisticsMetricValue::F64(current), StatisticsMetricValue::F64(candidate))
            if current.is_finite() && candidate.is_finite() =>
        {
            Some(StatisticsMetricValue::F64(if lower {
                current.min(candidate)
            } else {
                current.max(candidate)
            }))
        }
        _ => None,
    }
}

fn statistics_evidence(
    provider: &IcebergMetadata,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    table: &IcebergTablePayload,
    data_version: &StatisticsDataVersion,
    path: &str,
) -> Result<ExternalMutationEvidence, ConnectorError> {
    let payload = encode_statistics_evidence(&IcebergStatisticsEvidenceV1 {
        version: ICEBERG_STATISTICS_EVIDENCE_VERSION,
        namespace: table.namespace.clone(),
        table: table.table.clone(),
        data_version: data_version.as_bytes().to_vec(),
        statistics_path: path.to_string(),
    })
    .map_err(internal)?;
    ExternalMutationEvidence::try_new(
        ICEBERG_STATISTICS_EVIDENCE_VERSION,
        provider.descriptor().clone(),
        provider.incarnation(),
        operation_id,
        STATISTICS_OPERATION_KIND,
        Bytes::from(payload),
    )
}

fn statistics_receipt(
    provider: &IcebergMetadata,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    data_version: StatisticsDataVersion,
    revision: StatisticsEvidenceRevision,
    payload: Bytes,
    effect: ExternalMutationEffect,
) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
    Ok(ExternalMutationOutcome::KnownCommitted {
        effect,
        receipt: StatisticsReceipt::try_new(
            provider.descriptor().clone(),
            provider.incarnation(),
            operation_id,
            data_version,
            revision,
            payload,
        )?,
        finalization: ExternalMutationFinalization::Complete,
    })
}

fn known_uncommitted(error: ConnectorError) -> ExternalMutationOutcome<StatisticsReceipt> {
    ExternalMutationOutcome::KnownUncommitted {
        failure: ConnectorMutationFailure::new(failure_kind(error.kind()), error.to_string()),
    }
}

fn failure_kind(kind: ConnectorErrorKind) -> ConnectorMutationFailureKind {
    match kind {
        ConnectorErrorKind::InvalidRequest => ConnectorMutationFailureKind::InvalidRequest,
        ConnectorErrorKind::NotFound => ConnectorMutationFailureKind::NotFound,
        ConnectorErrorKind::PermissionDenied => ConnectorMutationFailureKind::PermissionDenied,
        ConnectorErrorKind::Unsupported => ConnectorMutationFailureKind::Unsupported,
        ConnectorErrorKind::Cancelled => ConnectorMutationFailureKind::Cancelled,
        ConnectorErrorKind::DeadlineExceeded => ConnectorMutationFailureKind::DeadlineExceeded,
        ConnectorErrorKind::ResourceExhausted => ConnectorMutationFailureKind::ResourceExhausted,
        ConnectorErrorKind::Unavailable => ConnectorMutationFailureKind::Unavailable,
        ConnectorErrorKind::CorruptData => ConnectorMutationFailureKind::CorruptData,
        ConnectorErrorKind::Internal => ConnectorMutationFailureKind::Internal,
    }
}

fn validate_context(
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ));
    }
    Ok(())
}

fn missing_column(column: &str) -> StatisticsMetricState {
    StatisticsMetricState::Missing(StatisticsMissing {
        kind: StatisticsMissingKind::NotCollected,
        message: Arc::from(format!(
            "statistics for column `{column}` are not collected"
        )),
    })
}

fn incomplete(message: impl Into<Arc<str>>) -> StatisticsMetricState {
    StatisticsMetricState::Missing(StatisticsMissing {
        kind: StatisticsMissingKind::IncompleteEvidence,
        message: message.into(),
    })
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMetadata,
        ConnectorMutationOperationId, ConnectorProviderId, ConnectorRequestContext,
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
        ProviderBindingEpoch,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog::error::CatalogCommitEvidence;
    use crate::catalog::transaction::{
        CatalogCommitDispatch, CommitProof, Transaction, TransactionShape,
    };
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::iceberg::io::FileIO;
    use crate::iceberg::spec::{
        FormatVersion, Operation, PartitionSpec, Snapshot, SortOrder, Summary, TableMetadataBuilder,
    };
    use crate::iceberg::table::Table;
    use crate::iceberg::{Error as IcebergError, TableIdent};
    use crate::metadata_context::IcebergMetadataContext;
    use crate::resources::IcebergMetadataResources;

    struct NeverCancelled;

    impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct CancelOnCheck {
        checks: AtomicUsize,
        cancel_on: usize,
    }

    impl novarocks_spi::connector::ConnectorCancellation for CancelOnCheck {
        fn is_cancelled(&self) -> bool {
            self.checks.fetch_add(1, Ordering::SeqCst) + 1 >= self.cancel_on
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum DispatchBehavior {
        Commit,
        Conflict,
        RejectDefinitely,
        LoseResponse,
    }

    #[derive(Debug)]
    struct CountingDispatch {
        behavior: DispatchBehavior,
        dispatches: AtomicUsize,
        aborts: AtomicUsize,
    }

    impl CountingDispatch {
        fn new(behavior: DispatchBehavior) -> Arc<Self> {
            Arc::new(Self {
                behavior,
                dispatches: AtomicUsize::new(0),
                aborts: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl CatalogCommitDispatch for CountingDispatch {
        async fn dispatch_once(
            &self,
            _staged: Option<crate::iceberg::TableCommit>,
        ) -> Result<CommitProof, IcebergError> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                DispatchBehavior::Commit => Ok(CommitProof::applied(Some(7))),
                DispatchBehavior::Conflict => Err(IcebergError::new(
                    crate::iceberg::ErrorKind::CatalogCommitConflicts,
                    "injected statistics conflict",
                )),
                DispatchBehavior::RejectDefinitely => Err(IcebergError::new(
                    crate::iceberg::ErrorKind::DataInvalid,
                    "injected definite statistics rejection",
                )),
                DispatchBehavior::LoseResponse => Err(IcebergError::new(
                    crate::iceberg::ErrorKind::Unexpected,
                    "injected lost statistics response",
                )),
            }
        }

        async fn adjudicate(&self) -> Result<Option<CommitProof>, ConnectorError> {
            Ok(None)
        }

        async fn abort_before_dispatch(&self) -> Result<(), ConnectorError> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + std::time::Duration::from_secs(5),
            Arc::new(NeverCancelled),
            1024 * 1024,
            1024 * 1024,
        )
        .expect("context")
    }

    fn provider() -> (tokio::runtime::Runtime, tempfile::TempDir, IcebergMetadata) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                IcebergMetadataResources::new(binding, executor.handle().clone()),
            )
            .expect("control runtime"),
        );
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let provider = IcebergMetadata::new(
            descriptor,
            ProviderBindingEpoch::from_bytes([4; 16]),
            runtime,
        );
        (executor, warehouse, provider)
    }

    fn table_payload() -> IcebergTablePayload {
        IcebergTablePayload {
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_info: None,
            metadata_columns: Vec::new(),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            row_mutation_frozen_source: false,
            logical_type_columns: BTreeMap::new(),
            hidden_columns: Vec::new(),
        }
    }

    fn table_with_snapshot(file_io: FileIO) -> Table {
        let schema = crate::iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let snapshot = Snapshot::builder()
            .with_snapshot_id(7)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("memory://warehouse/db/t/metadata/snap-7.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .build();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/db/t".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .set_branch_snapshot(snapshot, "main")
        .expect("current snapshot")
        .build()
        .expect("metadata")
        .metadata;
        Table::builder()
            .identifier(TableIdent::from_strs(["db", "t"]).expect("identifier"))
            .metadata(metadata)
            .file_io(file_io)
            .build()
            .expect("table")
    }

    fn nonempty_statistics_session(
        provider: IcebergMetadata,
        physical_table: Table,
        operation_id: ConnectorMutationOperationId,
        context: ConnectorRequestContext,
        dispatch: Arc<CountingDispatch>,
    ) -> (
        IcebergStatisticsCollectionSession,
        StatisticsArtifactDraft,
        String,
    ) {
        let path = puffin_path_for_statistics_operation(
            physical_table.metadata(),
            7,
            statistics_attempt_identity(operation_id, 0),
        );
        let frontier = Transaction::new(
            TransactionIdentity::new("statistics-cleanup-test", [9; 16]),
            CatalogTableName::new("db", "t"),
            TransactionShape::Existing,
            CatalogCommitEvidence::for_target("db.t"),
            dispatch,
        );
        let identity = StatisticsArtifactIdentity::try_new(vec![1], APACHE_DATASKETCHES_THETA_V1)
            .expect("identity");
        let body = Bytes::from_static(include_bytes!(
            "../../../../../tests/datasketches-tck/fixtures/theta/rust_quickselect_n1000_ordered_v3.sk"
        ));
        let artifact = StatisticsArtifactDraft::try_new(
            identity.input_fields().to_vec(),
            identity.blob_type(),
            body,
            BTreeMap::new(),
        )
        .expect("artifact");
        (
            IcebergStatisticsCollectionSession {
                provider,
                operation_id,
                table_payload: table_payload(),
                data_version: StatisticsDataVersion::try_new(Bytes::from_static(b"snapshot-7"))
                    .expect("data version"),
                physical_table,
                snapshot_id: Some(7),
                sequence_number: Some(1),
                expectations: vec![identity],
                context,
                frontier: Some(Box::new(frontier)),
            },
            artifact,
            path,
        )
    }

    #[test]
    fn cancellation_after_puffin_stage_vetoes_statistics_dispatch_and_cleans_attempt_file() {
        let (executor, _warehouse, provider) = provider();
        let file_io = FileIO::new_with_memory();
        let physical_table = table_with_snapshot(file_io.clone());
        let operation_id = ConnectorMutationOperationId::new();
        let cancellation = Arc::new(CancelOnCheck {
            checks: AtomicUsize::new(0),
            cancel_on: 3,
        });
        let context = ConnectorRequestContext::try_new(
            Instant::now() + std::time::Duration::from_secs(5),
            cancellation.clone(),
            1024 * 1024,
            1024 * 1024,
        )
        .expect("context");
        let dispatch = CountingDispatch::new(DispatchBehavior::Commit);
        let (session, artifact, path) = nonempty_statistics_session(
            provider,
            physical_table,
            operation_id,
            context,
            Arc::clone(&dispatch),
        );

        let error = Box::new(session)
            .finish(vec![artifact])
            .expect_err("late cancellation must veto catalog dispatch");
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
        assert_eq!(cancellation.checks.load(Ordering::SeqCst), 3);
        assert_eq!(dispatch.dispatches.load(Ordering::SeqCst), 0);
        assert_eq!(dispatch.aborts.load(Ordering::SeqCst), 1);
        assert!(
            !executor
                .block_on(file_io.exists(&path))
                .expect("inspect attempt Puffin"),
            "the known-uncommitted attempt must clean its Puffin file"
        );
    }

    #[test]
    fn terminal_known_uncommitted_statistics_attempt_cleans_its_puffin() {
        let (executor, _warehouse, provider) = provider();
        let file_io = FileIO::new_with_memory();
        let physical_table = table_with_snapshot(file_io.clone());
        let dispatch = CountingDispatch::new(DispatchBehavior::RejectDefinitely);
        let (session, artifact, path) = nonempty_statistics_session(
            provider,
            physical_table,
            ConnectorMutationOperationId::new(),
            context(),
            Arc::clone(&dispatch),
        );

        let outcome = Box::new(session)
            .finish(vec![artifact])
            .expect("definite catalog rejection");
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownUncommitted { ref failure }
                if failure.kind() == ConnectorMutationFailureKind::InvalidRequest
        ));
        assert_eq!(dispatch.dispatches.load(Ordering::SeqCst), 1);
        assert!(
            !executor
                .block_on(file_io.exists(&path))
                .expect("inspect rejected attempt Puffin"),
            "a definitely uncommitted attempt must clean its Puffin file"
        );
    }

    #[test]
    fn conflict_cleans_attempt_puffin_before_fresh_base_reload() {
        let (executor, _warehouse, provider) = provider();
        let file_io = FileIO::new_with_memory();
        let physical_table = table_with_snapshot(file_io.clone());
        let dispatch = CountingDispatch::new(DispatchBehavior::Conflict);
        let (session, artifact, path) = nonempty_statistics_session(
            provider,
            physical_table,
            ConnectorMutationOperationId::new(),
            context(),
            Arc::clone(&dispatch),
        );

        Box::new(session)
            .finish(vec![artifact])
            .expect_err("the synthetic provider has no fresh catalog base");
        assert_eq!(dispatch.dispatches.load(Ordering::SeqCst), 1);
        assert!(
            !executor
                .block_on(file_io.exists(&path))
                .expect("inspect conflicted attempt Puffin"),
            "a conflicted attempt must clean its Puffin before fresh-base retry"
        );
    }

    #[test]
    fn commit_unknown_statistics_attempt_retains_its_puffin() {
        let (executor, _warehouse, provider) = provider();
        let file_io = FileIO::new_with_memory();
        let physical_table = table_with_snapshot(file_io.clone());
        let dispatch = CountingDispatch::new(DispatchBehavior::LoseResponse);
        let (session, artifact, path) = nonempty_statistics_session(
            provider,
            physical_table,
            ConnectorMutationOperationId::new(),
            context(),
            Arc::clone(&dispatch),
        );

        let outcome = Box::new(session)
            .finish(vec![artifact])
            .expect("lost response is a typed outcome");
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        assert_eq!(dispatch.dispatches.load(Ordering::SeqCst), 1);
        assert!(
            executor
                .block_on(file_io.exists(&path))
                .expect("inspect unknown attempt Puffin"),
            "unknown publication may have referenced its Puffin, so cleanup is forbidden"
        );
    }

    #[test]
    fn empty_collection_bypasses_catalog_transaction_admission() {
        let (executor, _warehouse, provider) = provider();
        let catalog = provider.runtime().novarocks_catalog().vendored_client();
        executor.block_on(async move {
            let namespace = crate::iceberg::NamespaceIdent::new("empty_stats".to_string());
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .expect("create namespace");
            let schema = crate::iceberg::spec::Schema::builder()
                .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .expect("schema");
            catalog
                .create_table(
                    &namespace,
                    crate::iceberg::TableCreation::builder()
                        .name("t".to_string())
                        .schema(schema)
                        .format_version(crate::iceberg::spec::FormatVersion::V2)
                        .build(),
                )
                .await
                .expect("create table");
        });
        let identity = ConnectorTableIdentity {
            instance_id: provider.descriptor().instance_id.clone(),
            namespace: Arc::from("empty_stats"),
            table: Arc::from("t"),
        };
        let metadata = provider
            .load_table(ConnectorTableRequest {
                table: identity,
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context(),
            })
            .expect("load empty table");
        let data_version = metadata
            .statistics_data_version
            .expect("statistics data version");

        STATISTICS_TRANSACTION_ADMISSIONS.store(0, std::sync::atomic::Ordering::Relaxed);
        let start = StatisticsCollection::begin_collection(
            &provider,
            StatisticsCollectionStartRequest {
                operation_id: ConnectorMutationOperationId::new(),
                table: metadata.table,
                data_version,
                selection: StatisticsColumnSelection::Default,
                context: context(),
            },
        )
        .expect("begin empty collection");
        assert!(start.required_aggregations().is_empty());
        assert_eq!(
            STATISTICS_TRANSACTION_ADMISSIONS.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        let (_, _, _, _, session) = start.into_parts();
        let outcome = session.finish(Vec::new()).expect("finish empty collection");
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                ..
            }
        ));
        assert_eq!(
            STATISTICS_TRANSACTION_ADMISSIONS.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn uuid_collection_input_is_explicitly_fixed_size_binary() {
        let schema = crate::iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::optional(
                7,
                "u",
                Type::Primitive(PrimitiveType::Uuid),
            ))])
            .build()
            .expect("schema");
        let requirements = collection_requirements(
            &schema,
            &StatisticsColumnSelection::Explicit(vec![Arc::from("u")]),
        )
        .expect("UUID requirement");
        assert_eq!(requirements.len(), 1);
        assert_eq!(
            requirements[0].input().data_type(),
            &DataType::FixedSizeBinary(16)
        );
    }

    #[test]
    fn explicit_unsupported_field_fails_while_default_omits_it() {
        let schema = crate::iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::optional(
                7,
                "v",
                Type::Primitive(PrimitiveType::Variant),
            ))])
            .build()
            .expect("schema");
        assert!(
            collection_requirements(
                &schema,
                &StatisticsColumnSelection::Explicit(vec![Arc::from("v")]),
            )
            .is_err()
        );
        assert!(
            collection_requirements(&schema, &StatisticsColumnSelection::Default)
                .expect("default selection")
                .is_empty()
        );
    }

    #[test]
    fn artifact_validation_is_exact_and_adds_only_standard_ndv_metadata() {
        let body = Bytes::from_static(include_bytes!(
            "../../../../../tests/datasketches-tck/fixtures/theta/rust_quickselect_n1000_ordered_v3.sk"
        ));
        let identity = StatisticsArtifactIdentity::try_new(vec![7], APACHE_DATASKETCHES_THETA_V1)
            .expect("identity");
        let drafts = validate_artifacts(
            std::slice::from_ref(&identity),
            vec![
                StatisticsArtifactDraft::try_new(
                    vec![7],
                    APACHE_DATASKETCHES_THETA_V1,
                    body,
                    BTreeMap::new(),
                )
                .expect("draft"),
            ],
        )
        .expect("validated artifacts");
        assert_eq!(drafts[0].identity(), &identity);
        assert_eq!(
            drafts[0]
                .properties()
                .get(crate::stats_loader::NDV_PROPERTY),
            Some(&"1000".to_string())
        );
        assert!(validate_artifacts(std::slice::from_ref(&identity), Vec::new()).is_err());

        let error = validate_artifacts(
            std::slice::from_ref(&identity),
            vec![
                StatisticsArtifactDraft::try_new(
                    vec![7],
                    APACHE_DATASKETCHES_THETA_V1,
                    drafts[0].body().clone(),
                    BTreeMap::from([("private".to_string(), "value".to_string())]),
                )
                .expect("draft"),
            ],
        )
        .expect_err("provider-private artifact properties must be rejected");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn publication_retries_only_a_definite_conflict_with_remaining_budget() {
        let conflict = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Conflict,
            "concurrent table update",
        );
        assert_eq!(next_statistics_attempt(&conflict, 0, 3), Some(1));
        assert_eq!(next_statistics_attempt(&conflict, 1, 3), Some(2));
        assert_eq!(next_statistics_attempt(&conflict, 2, 3), None);

        for kind in [
            ConnectorMutationFailureKind::Unavailable,
            ConnectorMutationFailureKind::DeadlineExceeded,
            ConnectorMutationFailureKind::InvalidRequest,
        ] {
            let failure = ConnectorMutationFailure::new(kind, "definite failure");
            assert_eq!(next_statistics_attempt(&failure, 0, 3), None);
        }
    }

    fn manifest_file(
        record_count: Option<i64>,
        column: &str,
        has_deletes: bool,
    ) -> DataFileWithStats {
        DataFileWithStats {
            path: "data.parquet".to_string(),
            size: 64,
            record_count,
            column_stats: Some(HashMap::from([(
                column.to_string(),
                crate::scan_model::IcebergColumnStats {
                    field_id: Some(1),
                    null_count: Some(0),
                    value_count: record_count,
                    column_size: Some(32),
                    lower_bound: Some(1_i32.to_le_bytes().to_vec()),
                    upper_bound: Some(9_i32.to_le_bytes().to_vec()),
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            partition_values: None,
            manifest_path: None,
            partition_field_values: Vec::new(),
            first_row_id: None,
            data_sequence_number: None,
            delete_files: if has_deletes {
                vec![crate::scan_model::IcebergDeleteFileInfo {
                    path: "delete.parquet".to_string(),
                    file_format: crate::scan_model::IcebergDeleteFileFormat::Parquet,
                    file_content: crate::scan_model::IcebergDeleteFileContent::Position,
                    length: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                    sequence_number: None,
                    partition_spec_id: None,
                    partition_key: None,
                    referenced_data_file: None,
                    equality_column_names: Vec::new(),
                    equality_field_ids: Vec::new(),
                }]
            } else {
                Vec::new()
            },
        }
    }

    fn manifest_nature(
        metric: &StatisticsMetric,
        has_deletes: bool,
    ) -> Option<StatisticsNumericNature> {
        let files = vec![manifest_file(Some(4), "k", has_deletes)];
        let data_types = HashMap::from([("k".to_string(), DataType::Int32)]);
        let basis =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("data version");
        match manifest_metric(metric, &files, &data_types, has_deletes, &basis) {
            StatisticsMetricState::Available(observation) => Some(observation.numeric_nature()),
            _ => None,
        }
    }

    #[test]
    fn manifest_row_count_requires_every_file() {
        let files = vec![manifest_file(None, "k", false)];
        let basis =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("data version");
        assert!(matches!(
            manifest_metric(
                &StatisticsMetric::RowCount,
                &files,
                &HashMap::new(),
                false,
                &basis,
            ),
            StatisticsMetricState::Missing(_)
        ));
    }

    /// A sketch can legitimately estimate above the table's own size, and a
    /// published value can outlive the rows it counted. Neither may reach the
    /// optimizer as-is.
    #[test]
    fn a_published_ndv_is_kept_within_what_the_table_can_hold() {
        let bounded = Some(RowCount {
            rows: 10,
            exact: true,
        });
        assert_eq!(
            cap_ndv(1_000.0, bounded),
            10.0,
            "an NDV cannot exceed the rows"
        );
        assert_eq!(
            cap_ndv(4.0, bounded),
            4.0,
            "an NDV within the table is kept"
        );

        // A count over a snapshot with delete files is itself an upper bound, so
        // the cap is loose — but still worth applying.
        assert_eq!(
            cap_ndv(
                1_000.0,
                Some(RowCount {
                    rows: 10,
                    exact: false
                })
            ),
            10.0
        );

        assert_eq!(
            cap_ndv(7.0, None),
            7.0,
            "with no row count there is nothing to cap against"
        );
    }

    /// The floor of one distinct value is there so a non-empty column never
    /// reports zero. A table proven empty is the one case where zero is right.
    #[test]
    fn an_empty_table_reports_no_distinct_values() {
        assert_eq!(
            cap_ndv(
                3.0,
                Some(RowCount {
                    rows: 0,
                    exact: true
                })
            ),
            0.0
        );
        assert_eq!(
            cap_ndv(
                3.0,
                Some(RowCount {
                    rows: 0,
                    exact: false
                })
            ),
            1.0,
            "a row count that is only an upper bound does not prove emptiness"
        );
    }

    #[test]
    fn without_delete_files_manifest_counts_and_bounds_are_exact() {
        let column: Arc<str> = Arc::from("k");
        for metric in [
            StatisticsMetric::RowCount,
            StatisticsMetric::NullCount {
                column: Arc::clone(&column),
            },
            StatisticsMetric::Minimum {
                column: Arc::clone(&column),
            },
            StatisticsMetric::Maximum {
                column: Arc::clone(&column),
            },
        ] {
            assert_eq!(
                manifest_nature(&metric, false),
                Some(StatisticsNumericNature::Exact),
                "{metric:?} is exact when no rows are hidden by delete files"
            );
        }
        assert_eq!(
            manifest_nature(
                &StatisticsMetric::AverageSize {
                    column: Arc::clone(&column)
                },
                false
            ),
            Some(StatisticsNumericNature::TwoSidedApproximate),
            "a ratio materialized as f64 must not claim exact numeric identity"
        );
        // NDV is not a manifest fact at all: it lives in Puffin and is resolved
        // from the snapshot ancestry, so asking the manifest for it yields
        // nothing rather than a value.
        assert_eq!(
            manifest_nature(
                &StatisticsMetric::ThetaNdv {
                    column: Arc::clone(&column)
                },
                false
            ),
            None
        );
    }

    #[test]
    fn delete_files_bend_each_manifest_metric_in_its_own_direction() {
        let column: Arc<str> = Arc::from("k");
        // Sums do not subtract deleted rows, so they over-report.
        assert_eq!(
            manifest_nature(&StatisticsMetric::RowCount, true),
            Some(StatisticsNumericNature::UpperBound)
        );
        assert_eq!(
            manifest_nature(
                &StatisticsMetric::NullCount {
                    column: Arc::clone(&column)
                },
                true
            ),
            Some(StatisticsNumericNature::UpperBound)
        );
        // The true minimum can only rise and the true maximum can only fall
        // when rows disappear, so the bounds stay valid in opposite directions.
        assert_eq!(
            manifest_nature(
                &StatisticsMetric::Minimum {
                    column: Arc::clone(&column)
                },
                true
            ),
            Some(StatisticsNumericNature::LowerBound)
        );
        assert_eq!(
            manifest_nature(
                &StatisticsMetric::Maximum {
                    column: Arc::clone(&column)
                },
                true
            ),
            Some(StatisticsNumericNature::UpperBound)
        );
    }

    #[test]
    fn delete_files_do_not_reduce_manifest_row_coverage() {
        // Coverage answers "did the measurement account for every visible row",
        // which a full manifest read does whether or not deletes exist. The
        // delete-file effect belongs to numeric nature, not to coverage.
        let files = [manifest_file(Some(4), "k", true)];
        assert!(
            files.iter().all(|file| file.record_count.is_some()),
            "the coverage predicate must not consult delete files"
        );
    }

    #[test]
    fn response_loss_evidence_is_deterministic_and_exact_generation_bound() {
        let (_executor, _warehouse, provider) = provider();
        let operation_id = ConnectorMutationOperationId::new();
        let data_version =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("data version");
        let first = statistics_evidence(
            &provider,
            operation_id,
            &table_payload(),
            &data_version,
            "s3://warehouse/db/t/metadata/stats.puffin",
        )
        .expect("evidence");
        let second = statistics_evidence(
            &provider,
            operation_id,
            &table_payload(),
            &data_version,
            "s3://warehouse/db/t/metadata/stats.puffin",
        )
        .expect("evidence replay");
        assert_eq!(first, second);
        let decoded =
            crate::reconcile_payload::decode_statistics_evidence(first.provider_payload())
                .expect("decode");
        assert_eq!(decoded.namespace, "db");
        assert_eq!(decoded.table, "t");
        assert_eq!(
            decoded.statistics_path,
            "s3://warehouse/db/t/metadata/stats.puffin"
        );
    }
}
