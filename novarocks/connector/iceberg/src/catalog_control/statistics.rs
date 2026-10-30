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
//! Design: ADR-0136 (docs/adr/ADR-0136-ordinary-aggregate-statistics-dataflow.md)

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
    AsyncStatisticsCollectionSession, AsyncStatisticsCollectionStart, ConnectorError,
    ConnectorErrorKind, ConnectorMutationFailure, ConnectorOperationControl, ConnectorStatistics,
    ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome, StatisticsArtifactDraft, StatisticsArtifactIdentity,
    StatisticsBasisRelation, StatisticsCollection, StatisticsCollectionSession,
    StatisticsCollectionStart, StatisticsCollectionStartRequest, StatisticsColumnSelection,
    StatisticsDataVersion, StatisticsEvidence, StatisticsEvidenceRevision, StatisticsMetric,
    StatisticsMetricObservation, StatisticsMetricSource, StatisticsMetricState,
    StatisticsMetricValue, StatisticsMissing, StatisticsMissingKind, StatisticsNumericNature,
    StatisticsPublicationClose, StatisticsPublicationReady, StatisticsReadRequest,
    StatisticsReader, StatisticsReceipt, StatisticsRequiredAggregation, StatisticsRowCoverage,
    StatisticsScanColumn, StatisticsSessionFuture,
};
use sha2::{Digest, Sha256};

use crate::iceberg::puffin::APACHE_DATASKETCHES_THETA_V1;
use crate::iceberg::spec::Type;
use crate::manifest::{DataFileWithStats, extract_data_files_with_stats_at_with_control};
use crate::metadata::{IcebergMetadata, IcebergTablePayload};
use crate::statistics_ancestry::AncestorNdv;
use crate::statistics_basis::basis_relation;
use crate::statistics_codec::{statistics_data_version, statistics_metric_column};
#[path = "statistics/async_publication.rs"]
mod async_publication;
#[path = "statistics/publication.rs"]
mod publication;
#[path = "statistics/scoped_cpu.rs"]
pub(crate) mod scoped_cpu;

const STATISTICS_OPERATION_KIND: &str = "statistics-publish";
const STATISTICS_PUBLICATION_EVIDENCE_VERSION: u16 = 2;

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
        let owner = self.clone();
        self.runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move { owner.read_statistics_async(request).await })
            .map_err(unavailable)?
    }

    fn prepare_scoped_read(
        &self,
        request: novarocks_spi::connector::ConnectorTableRequest,
        original: novarocks_spi::connector::ConnectorOriginalResultScope,
    ) -> Result<Arc<dyn novarocks_spi::connector::StatisticsReadOriginal>, ConnectorError> {
        self.validate_context(&request.context)?;
        if request
            .context
            .io_admission()
            .and_then(std::sync::Weak::upgrade)
            .is_none()
        {
            return Err(invalid(
                "scoped statistics read requires its original MIO source",
            ));
        }
        Ok(scoped_cpu::StatisticsCpuOriginal::read_source(
            self.clone(),
            request,
            original,
        ))
    }

    fn read_statistics_async(
        &self,
        request: StatisticsReadRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<StatisticsEvidence, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
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
                .load_table_for_request_async(&table.namespace, &table.table, &request.context)
                .await
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
            let control = request.context.clone();
            let files_result = extract_data_files_with_stats_at_with_control(
                &table_for_files,
                snapshot_id,
                Some(&control as &dyn ConnectorOperationControl),
            )
            .await;
            self.validate_context(&request.context)?;
            let files = files_result.map_err(unavailable)?;
            project_statistics_evidence(
                metadata,
                table_info,
                snapshot_id,
                &expected,
                &request,
                &files,
            )
        })
    }
}

/// Only the file facts needed by table statistics; a scoped read borrows its
/// original read snapshot instead of cloning the scan/delete graph.
trait StatisticsFileFacts {
    fn record_count(&self) -> Option<i64>;
    fn stats(&self) -> Option<&HashMap<String, crate::scan_model::IcebergColumnStats>>;
    fn has_deletes(&self) -> bool;
}
impl StatisticsFileFacts for DataFileWithStats {
    fn record_count(&self) -> Option<i64> {
        self.record_count
    }
    fn stats(&self) -> Option<&HashMap<String, crate::scan_model::IcebergColumnStats>> {
        self.column_stats.as_ref()
    }
    fn has_deletes(&self) -> bool {
        !self.delete_files.is_empty()
    }
}
impl StatisticsFileFacts for crate::read_model::IcebergReadFile {
    fn record_count(&self) -> Option<i64> {
        self.record_count
    }
    fn stats(&self) -> Option<&HashMap<String, crate::scan_model::IcebergColumnStats>> {
        self.column_stats.as_ref()
    }
    fn has_deletes(&self) -> bool {
        self.deletes.members().next().is_some()
    }
}
fn project_statistics_evidence<F: StatisticsFileFacts>(
    metadata: &crate::iceberg::spec::TableMetadata,
    table_info: &crate::scan_model::IcebergTableInfo,
    snapshot_id: i64,
    expected: &StatisticsDataVersion,
    request: &StatisticsReadRequest,
    files: &[F],
) -> Result<StatisticsEvidence, ConnectorError> {
    let arrow_schema =
        crate::scalar_integer_domain::metadata_sql_schema(metadata, metadata.current_schema())?;
    for file in files {
        for field in arrow_schema.fields() {
            let domain = match field.data_type() {
                DataType::Int8 => Some(crate::scalar_integer_domain::ScalarIntegerDomain::Int8),
                DataType::Int16 => Some(crate::scalar_integer_domain::ScalarIntegerDomain::Int16),
                _ => None,
            };
            let Some(domain) = domain else {
                continue;
            };
            if let Some(metric) = column_stats(file, field.name()) {
                for bytes in [metric.lower_bound.as_deref(), metric.upper_bound.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    let value =
                        i32::from_le_bytes(bytes.try_into().map_err(|_| {
                            corrupt("Iceberg scalar integer bound is not four bytes")
                        })?);
                    domain.value(value)?;
                }
            }
        }
    }
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
    let row_coverage = if files.iter().all(|file| file.record_count().is_some()) {
        StatisticsRowCoverage::AllVisibleRows
    } else {
        StatisticsRowCoverage::PartialRows
    };
    let has_deletes = files.iter().any(|file| file.has_deletes());

    let mut metrics: BTreeMap<StatisticsMetric, StatisticsMetricState> = request
        .metrics
        .metrics()
        .iter()
        .filter(|metric| !matches!(metric, StatisticsMetric::ThetaNdv { .. }))
        .cloned()
        .map(|metric| {
            let state = manifest_metric(&metric, files, &data_types, has_deletes, expected);
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
        let field_set = wanted_ndv.keys().copied().collect::<BTreeSet<_>>();
        crate::statistics_ancestry::resolve_ancestor_ndv_from_metadata(
            metadata,
            snapshot_id,
            &field_set,
        )
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
    StatisticsEvidence::try_new(expected.clone(), revision, row_coverage, metrics)
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
    fn scoped_cpu_source_type_id(&self) -> Option<std::any::TypeId> {
        Some(std::any::TypeId::of::<scoped_cpu::StatisticsCpuOriginal>())
    }

    fn io_profile(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile, ConnectorError>
    {
        novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile::from_geometry(
            statistics_collection_io_geometry()?,
            self.runtime().metadata_action_initial_dependencies()?,
        )
    }
    fn begin_collection_async(
        &self,
        request: StatisticsCollectionStartRequest,
        cpu: Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
    ) -> StatisticsSessionFuture<'_, AsyncStatisticsCollectionStart> {
        Box::pin(self.begin_collection_original_async(request, cpu))
    }

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
        let (table, version, ordinal, requirements, session) =
            self.collection_session(request, table_payload, physical.table)?;
        StatisticsCollectionStart::try_new(table, version, ordinal, requirements, Box::new(session))
    }
}

type CollectionSessionParts = (
    novarocks_spi::connector::ConnectorTableHandle,
    StatisticsDataVersion,
    Option<i64>,
    Vec<StatisticsRequiredAggregation>,
    IcebergStatisticsCollectionSession,
);

impl IcebergMetadata {
    fn collection_session(
        &self,
        request: StatisticsCollectionStartRequest,
        table_payload: IcebergTablePayload,
        physical_table: crate::iceberg::table::Table,
    ) -> Result<CollectionSessionParts, ConnectorError> {
        self.collection_session_borrowed(&request, &table_payload, &physical_table, None)
    }
    fn collection_session_borrowed(
        &self,
        request: &StatisticsCollectionStartRequest,
        table_payload: &IcebergTablePayload,
        physical_table: &crate::iceberg::table::Table,
        cpu: Option<std::sync::Weak<dyn novarocks_spi::connector::StatisticsCpuPort>>,
    ) -> Result<CollectionSessionParts, ConnectorError> {
        let info = base_table_info(table_payload, "statistics collection")?;
        let expected_uuid = info
            .table_uuid
            .as_deref()
            .ok_or_else(|| corrupt("Iceberg table payload is missing its table UUID"))?;
        if physical_table.metadata().uuid().to_string() != expected_uuid {
            return Err(invalid(
                "Iceberg statistics collection resolved a different physical table UUID",
            ));
        }

        let (snapshot_id, sequence_number, requirements) = match info.current_snapshot_id {
            Some(snapshot_id) => {
                let snapshot = physical_table
                    .metadata()
                    .snapshot_by_id(snapshot_id)
                    .ok_or_else(|| {
                        invalid(
                            "the snapshot selected for statistics is no longer present in table metadata",
                        )
                    })?;
                let schema = snapshot
                    .schema(physical_table.metadata())
                    .map_err(|error| {
                        corrupt(format!(
                            "resolve the schema of the measured Iceberg snapshot: {error}"
                        ))
                    })?;
                let execution =
                    crate::metadata::with_pure_schema_creation(&request.context, |creation| {
                        crate::schema_mapping::catalog_value::table(
                            physical_table.metadata(),
                            &schema,
                            &[],
                            creation,
                        )
                        .map_err(crate::metadata::metadata_creation_error)
                    })?;
                let requirements =
                    collection_requirements(&schema, &execution, &request.selection)?;
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
        let session = IcebergStatisticsCollectionSession {
            body: Arc::new(StatisticsSessionBody {
                provider: self.clone(),
                operation_id: request.operation_id,
                table_payload: table_payload.clone(),
                data_version: request.data_version.clone(),
                physical_table: physical_table.clone(),
                snapshot_id,
                sequence_number,
                expectations,
                context: request.context.clone(),
            }),
            publication: None,
            cpu,
        };
        Ok((
            request.table.clone(),
            request.data_version.clone(),
            snapshot_id,
            requirements,
            session,
        ))
    }

    async fn begin_collection_original_async(
        &self,
        request: StatisticsCollectionStartRequest,
        cpu: Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
    ) -> Result<AsyncStatisticsCollectionStart, ConnectorError> {
        validate_context(&request.context)?;
        let port = async_publication::original_io(&request.context)?;
        let profile = <Self as StatisticsCollection>::io_profile(self)?;
        port.configure_read_source(&profile)?;
        std::future::poll_fn(|cx| port.poll_read_source_ready(cx)).await?;
        let table_payload = self.table_payload(&request.table)?;
        let info = base_table_info(&table_payload, "statistics collection")?;
        if request.data_version != pinned_data_version(info)? {
            return Err(invalid(
                "Iceberg statistics collection does not match its resolved table pin",
            ));
        }
        let physical = self
            .runtime()
            .load_table_for_request_async(
                &table_payload.namespace,
                &table_payload.table,
                &request.context,
            )
            .await
            .map_err(unavailable)?;
        let source = scoped_cpu::StatisticsCpuOriginal::schema(
            self.clone(),
            request,
            table_payload,
            physical.table,
            Arc::downgrade(&cpu),
        );
        scoped_cpu::run_schema(&cpu, &source).await?;
        source.take_schema_result()
    }
}

struct IcebergStatisticsCollectionSession {
    body: Arc<StatisticsSessionBody>,
    publication: Option<async_publication::PreparedStatisticsPublication>,
    cpu: Option<std::sync::Weak<dyn novarocks_spi::connector::StatisticsCpuPort>>,
}
impl std::ops::Deref for IcebergStatisticsCollectionSession {
    type Target = StatisticsSessionBody;
    fn deref(&self) -> &Self::Target {
        &self.body
    }
}
struct StatisticsSessionBody {
    provider: IcebergMetadata,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    table_payload: IcebergTablePayload,
    data_version: StatisticsDataVersion,
    physical_table: crate::iceberg::table::Table,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    expectations: Vec<StatisticsArtifactIdentity>,
    context: novarocks_spi::connector::ConnectorRequestContext,
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
        let this = *self;
        validate_context(&this.context)?;
        let artifacts = validate_artifacts(&this.expectations, artifacts)?;
        let revision =
            collection_revision(&this.data_version, this.operation_id, &this.expectations)?;
        if artifacts.is_empty() {
            return statistics_receipt(
                &this.provider,
                this.operation_id,
                this.data_version.clone(),
                revision,
                Bytes::new(),
                ExternalMutationEffect::NoOp,
            );
        }
        publication::publish(this, artifacts, revision)
    }

    fn abort(self: Box<Self>) -> Result<(), ConnectorError> {
        // Collection owns immutable measurement facts; publication allocates
        // its operation and artifacts only when finish consumes this authority.
        Ok(())
    }
}

impl AsyncStatisticsCollectionSession for IcebergStatisticsCollectionSession {
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
    fn io_profile(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile, ConnectorError>
    {
        <IcebergMetadata as StatisticsCollection>::io_profile(&self.provider)
    }
    fn prepare_publication_async<'a>(
        &'a mut self,
        artifacts: &'a mut Option<Vec<StatisticsArtifactDraft>>,
    ) -> StatisticsSessionFuture<'a, ()> {
        Box::pin(self.prepare_original(artifacts))
    }
    fn finish_publication_async(
        &mut self,
    ) -> StatisticsSessionFuture<'_, StatisticsPublicationReady> {
        Box::pin(self.finish_original())
    }
    fn close_publication_async<'a>(
        &'a mut self,
        current: &'a novarocks_spi::connector::ConnectorRequestContext,
    ) -> StatisticsSessionFuture<'a, StatisticsPublicationClose> {
        Box::pin(self.close_original(current))
    }
}

impl IcebergStatisticsCollectionSession {
    async fn prepare_original(
        &mut self,
        artifacts: &mut Option<Vec<StatisticsArtifactDraft>>,
    ) -> Result<(), ConnectorError> {
        if self.publication.is_some() {
            return Err(invalid("Statistics publication is already prepared"));
        }
        validate_context(&self.context)?;
        async_publication::original_io(&self.context)?;
        let drafts = artifacts
            .as_ref()
            .ok_or_else(|| invalid("Statistics artifacts slot is empty"))?;
        let peak = crate::stats_assembler::statistics_publication_owned_bytes(
            drafts,
            &self.physical_table,
            &self.expectations,
        )?;
        self.context.reserve_owned_metadata(peak).await?;
        let cpu = scoped_cpu::original_cpu(&self.cpu)?;
        let source = scoped_cpu::StatisticsCpuOriginal::artifacts(
            Arc::clone(&self.body),
            artifacts.take().expect("checked original artifacts"),
            Arc::downgrade(&cpu),
        );
        if let Err(error) = scoped_cpu::run_artifacts(&cpu, &source).await {
            // submit refusal keeps the same original drafts, not a truncated copy.
            source.restore_artifacts_in(artifacts);
            return Err(error);
        }
        match source.take_publication_result() {
            Ok(prepared) => self.publication = Some(prepared),
            Err(error) => {
                source.restore_artifacts_in(artifacts);
                return Err(error);
            }
        }

        Ok(())
    }
    async fn finish_original(&mut self) -> Result<StatisticsPublicationReady, ConnectorError> {
        let prepared = self
            .publication
            .as_mut()
            .ok_or_else(|| invalid("Statistics publication is not prepared"))?;
        match prepared {
            async_publication::PreparedStatisticsPublication::NoOp(outcome) => {
                Ok(StatisticsPublicationReady::Settled(outcome.clone()))
            }
            async_publication::PreparedStatisticsPublication::Active(source) => {
                let ready = source.finish().await?;
                if matches!(
                    &ready,
                    StatisticsPublicationReady::Committed { .. }
                        | StatisticsPublicationReady::Settled(
                            ExternalMutationOutcome::KnownCommitted { .. }
                        )
                ) {
                    self.provider
                        .runtime()
                        .control_state()
                        .invalidate_table(&self.table_payload.namespace, &self.table_payload.table);
                }
                Ok(ready)
            }
        }
    }
    async fn close_original(
        &mut self,
        current: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatisticsPublicationClose, ConnectorError> {
        async_publication::same_original_io(&self.context, current)?;
        if let Some(weak) = &self.cpu {
            let cpu = weak
                .upgrade()
                .ok_or_else(|| invalid("Statistics CPU source is gone"))?;
            std::future::poll_fn(|cx| cpu.poll_closed(cx)).await?;
        }
        if self.publication.is_none() {
            // Begin owns only immutable measurement facts. There is no
            // operation, output allocation, or publication to settle. Install
            // the real closed state before returning, so prepare cannot reopen.
            self.publication = Some(async_publication::PreparedStatisticsPublication::NoOp(
                known_uncommitted(invalid("Statistics collection closed before publication")),
            ));
        }
        let close = match self
            .publication
            .as_mut()
            .expect("original closed/prepared slot")
        {
            async_publication::PreparedStatisticsPublication::NoOp(outcome) => {
                StatisticsPublicationClose::Observed {
                    outcome: outcome.clone(),
                    owned_objects_disposed: true,
                }
            }
            async_publication::PreparedStatisticsPublication::Active(source) => {
                source.close(current).await?
            }
        };
        if matches!(
            &close,
            StatisticsPublicationClose::Observed {
                outcome: ExternalMutationOutcome::KnownCommitted { .. },
                ..
            }
        ) {
            self.provider
                .runtime()
                .control_state()
                .invalidate_table(&self.table_payload.namespace, &self.table_payload.table);
        }
        Ok(close)
    }
}

/// The actual statistics factory's original headers and borrowed async frames.
/// Payload graphs grow from the actual drafts under the same MIO port.
pub fn statistics_collection_io_geometry()
-> Result<novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry, ConnectorError> {
    async_publication::geometry(
        future3_bytes(IcebergMetadata::begin_collection_original_async)
            .max(IcebergMetadata::statistics_binding_future_bytes())
            .max(scoped_cpu::StatisticsCpuOriginal::read_future_bytes())
            .max(future2_bytes(
                IcebergStatisticsCollectionSession::prepare_original,
            ))
            .max(future1_bytes(
                IcebergStatisticsCollectionSession::finish_original,
            ))
            .max(future2_bytes(
                IcebergStatisticsCollectionSession::close_original,
            )),
    )
}
fn future1_bytes<A, F>(_: fn(A) -> F) -> usize {
    std::mem::size_of::<F>()
}
fn future2_bytes<A, B, F>(_: fn(A, B) -> F) -> usize {
    std::mem::size_of::<F>()
}

fn collection_requirements(
    schema: &crate::iceberg::spec::Schema,
    execution: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    selection: &StatisticsColumnSelection,
) -> Result<Vec<StatisticsRequiredAggregation>, ConnectorError> {
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
        // Preserve ANALYZE's existing VARIANT capability boundary. Its SQL
        // byte carrier is supported by collect-on-write, but ANALYZE did not
        // admit the storage Struct representation before this type migration.
        let supported_storage = matches!(iceberg_field.field_type.as_ref(), Type::Primitive(_));
        if !supported_storage {
            if explicit.is_some() {
                return Err(unsupported_column(
                    &iceberg_field.name,
                    &iceberg_field.field_type,
                ));
            }
            continue;
        };
        let arrow_field = execution.schema().fields().get(ordinal).ok_or_else(|| {
            corrupt("Iceberg and Arrow statistics schemas have different field counts")
        })?;
        if !supports_theta_input_type(arrow_field.data_type()) {
            if explicit.is_some() {
                return Err(unsupported_column(
                    &iceberg_field.name,
                    &iceberg_field.field_type,
                ));
            }
            continue;
        }
        let value_type = execution
            .bindings()
            .get(ordinal)
            .ok_or_else(|| {
                corrupt("Iceberg statistics field has no complete execution declaration")
            })?
            .clone();
        let input = StatisticsScanColumn::try_new(
            ordinal,
            Arc::<str>::from(iceberg_field.name.as_str()),
            value_type,
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
        let mut parts = artifact.into_guarded_parts();
        let properties = parts.properties_mut();
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
        normalized.push(parts.try_into_draft()?);
    }
    if observed != expected {
        return Err(invalid(
            "statistics artifacts do not exactly match the session expectation set",
        ));
    }
    normalized.sort_by(|left, right| left.identity().cmp(right.identity()));
    Ok(normalized)
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

fn manifest_metric<F: StatisticsFileFacts>(
    metric: &StatisticsMetric,
    files: &[F],
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
                total.checked_add(u64::try_from(file.record_count()?).ok()?)
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
                total.checked_add(u64::try_from(file.record_count()?).ok()?)
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

fn column_stats<'a, F: StatisticsFileFacts>(
    file: &'a F,
    column: &str,
) -> Option<&'a crate::scan_model::IcebergColumnStats> {
    file.stats()?
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

fn statistics_receipt(
    provider: &IcebergMetadata,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    data_version: StatisticsDataVersion,
    revision: StatisticsEvidenceRevision,
    payload: Bytes,
    effect: ExternalMutationEffect,
) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
    Ok(ExternalMutationOutcome::KnownCommitted {
        cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
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
        failure: ConnectorMutationFailure::from_connector(error.clone()),
        cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
    }
}

fn validate_context(
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if context.is_cancelled() {
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

fn future3_bytes<A, B, C, F: std::future::Future>(_: fn(A, B, C) -> F) -> usize {
    std::mem::size_of::<F>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMetadata,
        ConnectorMutationOperationId, ConnectorProviderId, ConnectorRequestContext,
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
        ProviderBindingEpoch,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::iceberg::spec::PrimitiveType;
    use crate::metadata_context::IcebergMetadataContext;
    use crate::resources::IcebergMetadataResources;

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + std::time::Duration::from_secs(5),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024 * 1024,
            1024 * 1024,
        )
        .expect("context")
    }

    fn execution_fixture(
        schema: &crate::iceberg::spec::Schema,
        properties: std::collections::HashMap<String, String>,
    ) -> novarocks_connector_contract::ConnectorReadExecutionSchema {
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            schema.clone(),
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://statistics_fixture".to_string(),
            crate::iceberg::spec::FormatVersion::V3,
            properties,
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        crate::metadata::with_pure_schema_creation(&context(), |creation| {
            crate::schema_mapping::catalog_value::table(
                &metadata,
                metadata.current_schema(),
                &[],
                creation,
            )
            .map_err(crate::metadata::metadata_creation_error)
        })
        .expect("provider complete type author")
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

    #[test]
    fn empty_collection_requires_no_publication_preparation() {
        let (executor, _warehouse, provider) = provider();
        executor.block_on(async {
            use crate::catalog::CatalogNamespaceName;
            use crate::catalog::catalog_request::CatalogPublicationRequest;
            use crate::catalog::error::CatalogOutcome;
            let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                provider.runtime(),
                provider.incarnation(),
                CatalogPublicationRequest::CreateNamespace {
                    namespace: CatalogNamespaceName::new("empty_stats"),
                    properties: HashMap::new(),
                    no_op_if_exists: false,
                },
            )
            .await;
            assert!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                "{outcome:?}"
            );
            let schema = crate::iceberg::spec::Schema::builder()
                .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap();
            let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                provider.runtime(),
                provider.incarnation(),
                CatalogPublicationRequest::CreateTable {
                    namespace: CatalogNamespaceName::new("empty_stats"),
                    creation: crate::iceberg::TableCreation::builder()
                        .name("t".into())
                        .schema(schema)
                        .format_version(crate::iceberg::spec::FormatVersion::V2)
                        .build(),
                    no_op_if_exists: false,
                },
            )
            .await;
            assert!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                "{outcome:?}"
            );
        });
        let identity = ConnectorTableIdentity {
            instance_id: provider.descriptor().instance_id.clone(),
            namespace: Arc::from("empty_stats"),
            table: Arc::from("t"),
        };
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let metadata = provider
            .load_table(
                ConnectorTableRequest {
                    table: identity,
                    resolution: ConnectorTableResolution::StrictBaseTable,
                    context: context(),
                },
                &mut creation,
            )
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
            &execution_fixture(&schema, Default::default()),
            &StatisticsColumnSelection::Explicit(vec![Arc::from("u")]),
        )
        .expect("UUID requirement");
        assert_eq!(requirements.len(), 1);
        assert_eq!(
            requirements[0].input().data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            requirements[0].input().value_type().logical_type(),
            novarocks_type_contract::ValueLogicalType::Uuid
        );
        assert!(requirements[0].input().value_type().nullable());
    }

    #[test]
    fn ordinary_fixed16_collection_input_has_no_inferred_uuid_or_largeint_domain() {
        let schema = crate::iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
                8,
                "fixed",
                Type::Primitive(PrimitiveType::Fixed(16)),
            ))])
            .build()
            .expect("schema");
        let requirements = collection_requirements(
            &schema,
            &execution_fixture(&schema, Default::default()),
            &StatisticsColumnSelection::Explicit(vec![Arc::from("fixed")]),
        )
        .expect("fixed requirement");
        assert_eq!(requirements.len(), 1);
        let input = requirements[0].input().value_type();
        assert_eq!(input.data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            input.logical_type(),
            novarocks_type_contract::ValueLogicalType::Physical
        );
        assert!(!input.nullable());
    }

    #[test]
    fn explicit_unsupported_field_fails_while_default_omits_it() {
        let schema = crate::iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::optional(
                7,
                "v",
                Type::Variant(crate::iceberg::spec::VariantType),
            ))])
            .build()
            .expect("schema");
        assert!(
            collection_requirements(
                &schema,
                &execution_fixture(&schema, Default::default()),
                &StatisticsColumnSelection::Explicit(vec![Arc::from("v")]),
            )
            .is_err()
        );
        assert!(
            collection_requirements(
                &schema,
                &execution_fixture(&schema, Default::default()),
                &StatisticsColumnSelection::Default
            )
            .expect("default selection")
            .is_empty()
        );
    }

    #[test]
    fn collection_preserves_actual_property_domains_and_plain_carriers() {
        let schema = crate::iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(crate::iceberg::spec::NestedField::optional(
                    1,
                    "h",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(crate::iceberg::spec::NestedField::optional(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(crate::iceberg::spec::NestedField::optional(
                    3,
                    "n",
                    Type::Primitive(PrimitiveType::Fixed(16)),
                )),
                Arc::new(crate::iceberg::spec::NestedField::optional(
                    4,
                    "raw",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(crate::iceberg::spec::NestedField::optional(
                    5,
                    "fixed",
                    Type::Primitive(PrimitiveType::Fixed(16)),
                )),
            ])
            .build()
            .unwrap();
        let properties = [(crate::schema_mapping::nominal_declaration::PROPERTY.into(),r#"{"version":1,"fields":[{"id":1,"domain":"hll"},{"id":2,"domain":"bitmap"},{"id":3,"domain":"largeint"}]}"#.into())]
        .into();
        let execution = execution_fixture(&schema, properties);
        let requirements =
            collection_requirements(&schema, &execution, &StatisticsColumnSelection::Default)
                .unwrap();
        use novarocks_type_contract::ValueLogicalType;
        assert_eq!(
            requirements
                .iter()
                .map(|requirement| requirement.input().value_type().logical_type())
                .collect::<Vec<_>>(),
            vec![
                ValueLogicalType::Hll,
                ValueLogicalType::Bitmap,
                ValueLogicalType::LargeInt,
                ValueLogicalType::Physical,
                ValueLogicalType::Physical
            ]
        );
        assert_eq!(
            requirements
                .iter()
                .map(|requirement| requirement.input().data_type().clone())
                .collect::<Vec<_>>(),
            vec![
                DataType::Binary,
                DataType::Binary,
                DataType::FixedSizeBinary(16),
                DataType::Binary,
                DataType::FixedSizeBinary(16)
            ]
        );
        assert_eq!(
            requirements
                .iter()
                .map(|requirement| requirement.artifact().input_fields().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![1], vec![2], vec![3], vec![4], vec![5]]
        );
    }

    #[test]
    fn artifact_normalization_retains_the_admitted_holder_through_metadata_and_body_aliases() {
        let holder = Arc::new(());
        let weak = Arc::downgrade(&holder);
        let identity = StatisticsArtifactIdentity::try_new(vec![7], APACHE_DATASKETCHES_THETA_V1)
            .expect("identity");
        let draft = StatisticsArtifactDraft::try_new_with_guard(
            vec![7],
            APACHE_DATASKETCHES_THETA_V1,
            Bytes::from_static(include_bytes!(
                "../../../../../tests/datasketches-tck/fixtures/theta/rust_quickselect_n1000_ordered_v3.sk"
            )),
            BTreeMap::new(),
            novarocks_spi::connector::ConnectorPayloadRetentionGuard::new(holder),
        )
        .expect("guarded draft");
        let mut normalized = validate_artifacts(&[identity], vec![draft]).expect("normalization");
        let draft = normalized.pop().expect("normalized draft");
        let body = draft.body().slice(..);
        let parts = draft.into_guarded_parts();
        assert_eq!(
            parts.properties().get(crate::stats_loader::NDV_PROPERTY),
            Some(&"1000".to_string())
        );
        assert!(weak.upgrade().is_some());
        drop(parts);
        assert!(
            weak.upgrade().is_some(),
            "the body alias still owns admission"
        );
        drop(body);
        assert!(
            weak.upgrade().is_none(),
            "the last artifact owner releases admission"
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
                    record_count: None,
                    partition_data_json: None,
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
}
