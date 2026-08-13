#![allow(dead_code)]
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

use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_spi::connector::{
    ConnectorControlResolver, StatisticsMetric, StatisticsMetricRequest, StatisticsMetricState,
    StatisticsMetricValue, StatisticsProvenance,
};

use crate::connector::unified_statistics::{
    ResolvedStatisticsTable, StatisticsResolutionFailure, UnifiedStatisticsResolver,
};
use crate::engine::domain::{
    DmlExecutionKernel, MvExecutionKernel, QueryPreparationKernel, StatisticsExecutionKernel,
};
use crate::engine::query_planning::bindings::{
    QueryScanMaterialization, QueryTableBinding, QueryTableBindingAdmission,
    QueryTableBindingStore, parse_time_travel_overlay_identity,
};
use crate::engine::query_planning::catalog_materializer::{
    QueryTableBindingLoader, connector_query_binding_from_materialization,
    load_connector_table_alias_materialization_with_lease,
    load_connector_table_materialization_with_lease,
};
use crate::sql::catalog::ResolvedAnalyzerTable;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::statistics::Confidence;
use crate::sql::optimizer::stats_input::{
    BaseColumnStatistics, BaseTableStatistics, QueryStatsSnapshot, SqlStatisticsFatalError,
    SqlStatisticsSnapshot, SqlTableStatisticsEvidence, StatValue, StatsMissingReason, StatsRef,
    StatsSource,
};
use crate::sql::planner::table::{
    ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector,
};

#[derive(Clone, Default)]
/// Query-scoped handles for the one unified statistics resolver.  This is not
/// a provider registry: absent pins intentionally produce missing statistics
/// rather than a second latest-resolution path.
pub(crate) struct QueryStatisticsContext {
    bindings: Option<Arc<QueryTableBindingStore>>,
    snapshot: Arc<SqlStatisticsSnapshot>,
    resolver: Option<Arc<UnifiedStatisticsResolver>>,
}

impl QueryStatisticsContext {
    pub(crate) fn none() -> Self {
        Self::default()
    }

    pub(crate) fn unavailable() -> Self {
        Self::none()
    }

    pub(crate) fn from_statistics_resolver_with_bindings(
        resolver: &impl QueryStatisticsResolver,
        bindings: Arc<QueryTableBindingStore>,
    ) -> Self {
        Self {
            snapshot: Arc::new(project_statistics_snapshot(
                resolver.unified_statistics(),
                &bindings,
            )),
            bindings: Some(bindings),
            resolver: Some(Arc::clone(resolver.unified_statistics_arc())),
        }
    }

    pub(crate) fn from_standalone_state_with_bindings(
        state: &Arc<super::StandaloneState>,
        bindings: Arc<QueryTableBindingStore>,
    ) -> Self {
        Self::from_statistics_resolver_with_bindings(state, bindings)
    }

    pub(crate) fn from_optional_state_with_bindings(
        state: Option<&Arc<super::StandaloneState>>,
        bindings: Option<Arc<QueryTableBindingStore>>,
    ) -> Self {
        match (state, bindings) {
            (Some(state), Some(bindings)) => {
                Self::from_standalone_state_with_bindings(state, bindings)
            }
            // A caller without resolution pins must not resolve `latest` a
            // second time. Keep normal missing-statistics fallback instead.
            (Some(_), None) => Self::none(),
            (None, _) => Self::none(),
        }
    }
}

/// Query planning needs only frozen statistics evidence.  This trait avoids
/// taking the full application state while preserving the no-latest-lookup
/// rule in `QueryStatisticsContext`.
pub(crate) trait QueryStatisticsResolver {
    fn unified_statistics(&self) -> &UnifiedStatisticsResolver;
    fn unified_statistics_arc(&self) -> &Arc<UnifiedStatisticsResolver>;
}

impl QueryStatisticsResolver for super::StandaloneState {
    fn unified_statistics(&self) -> &UnifiedStatisticsResolver {
        self.unified_statistics.as_ref()
    }

    fn unified_statistics_arc(&self) -> &Arc<UnifiedStatisticsResolver> {
        &self.unified_statistics
    }
}

impl QueryStatisticsResolver for Arc<super::StandaloneState> {
    fn unified_statistics(&self) -> &UnifiedStatisticsResolver {
        self.as_ref().unified_statistics()
    }

    fn unified_statistics_arc(&self) -> &Arc<UnifiedStatisticsResolver> {
        self.as_ref().unified_statistics_arc()
    }
}

macro_rules! impl_kernel_statistics_resolver {
    ($kernel:ty) => {
        impl QueryStatisticsResolver for $kernel {
            fn unified_statistics(&self) -> &UnifiedStatisticsResolver {
                self.unified_statistics().as_ref()
            }

            fn unified_statistics_arc(&self) -> &Arc<UnifiedStatisticsResolver> {
                self.unified_statistics()
            }
        }
    };
}

impl_kernel_statistics_resolver!(QueryPreparationKernel);
impl_kernel_statistics_resolver!(DmlExecutionKernel);
impl_kernel_statistics_resolver!(MvExecutionKernel);
impl_kernel_statistics_resolver!(StatisticsExecutionKernel);

/// Project every admission-frozen connector observation into SQL values before
/// optimization begins.  This is the one application boundary that may touch
/// a lease, a table handle, or a connector capability; `QueryStatisticsContext`
/// subsequently serves only the immutable snapshot below.
fn project_statistics_snapshot(
    resolver: &UnifiedStatisticsResolver,
    bindings: &QueryTableBindingStore,
) -> SqlStatisticsSnapshot {
    let mut snapshot = SqlStatisticsSnapshot::empty();
    for (binding_id, binding) in bindings.captured_bindings() {
        let label = binding.resolved.catalog.identity.fqn();
        match project_binding_statistics(resolver, &binding) {
            Ok(statistics) => {
                snapshot.insert(binding_id, SqlTableStatisticsEvidence { label, statistics })
            }
            Err(error) => snapshot.insert_fatal(binding_id, error),
        }
    }
    snapshot
}

fn project_binding_statistics(
    resolver: &UnifiedStatisticsResolver,
    binding: &QueryTableBinding,
) -> Result<BaseTableStatistics, SqlStatisticsFatalError> {
    let Some(pin) = binding.statistics_pin.as_ref() else {
        return Ok(BaseTableStatistics::missing(
            StatsMissingReason::ConnectorUnsupported(
                "resolved table does not expose connector statistics".to_string(),
            ),
        ));
    };
    let planning_lease = binding
        .admission
        .exact_planning_lease()
        .map_err(|_| SqlStatisticsFatalError::BindingMissing)?;
    let control_binding = planning_lease.binding();
    if control_binding.descriptor().instance_id != *pin.table.owner() {
        return Err(SqlStatisticsFatalError::OwnerMismatch);
    }
    let Some(statistics) = control_binding.statistics() else {
        return Ok(BaseTableStatistics::missing(
            StatsMissingReason::ConnectorUnsupported(
                "resolved connector generation does not expose statistics".to_string(),
            ),
        ));
    };
    let metrics = metric_request(&binding.resolved.planner.columns).map_err(|error| {
        SqlStatisticsFatalError::CorruptEvidence(format!("build metric request: {error}"))
    })?;
    let context = crate::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
    .map_err(|error| {
        SqlStatisticsFatalError::CorruptEvidence(format!("build statistics context: {error}"))
    })?;
    let evidence = match resolver.resolve(
        &ResolvedStatisticsTable {
            table: pin.table.clone(),
            data_version: pin.data_version.clone(),
            incarnation: control_binding.incarnation(),
        },
        statistics.as_ref(),
        metrics,
        context,
    ) {
        Ok(evidence) => evidence,
        // A provider that cannot supply evidence remains the normal
        // conservative path.  Only a fact that contradicts the retained
        // binding is fatal to compilation.
        Err(StatisticsResolutionFailure::Connector(error)) => {
            return Ok(BaseTableStatistics::missing(
                StatsMissingReason::CatalogLoadError(error.to_string()),
            ));
        }
        Err(error) => return Err(map_resolution_failure(error)),
    };
    Ok(evidence_to_base_statistics(
        &evidence,
        &binding.resolved.planner.columns,
    ))
}

fn map_resolution_failure(error: StatisticsResolutionFailure) -> SqlStatisticsFatalError {
    match error {
        StatisticsResolutionFailure::OwnerMismatch => SqlStatisticsFatalError::OwnerMismatch,
        StatisticsResolutionFailure::IncarnationMismatch => {
            SqlStatisticsFatalError::IncarnationMismatch
        }
        StatisticsResolutionFailure::DataVersionMismatch => {
            SqlStatisticsFatalError::DataVersionMismatch
        }
        StatisticsResolutionFailure::CorruptEvidence(message) => {
            SqlStatisticsFatalError::CorruptEvidence(message)
        }
        StatisticsResolutionFailure::Connector(error) => SqlStatisticsFatalError::CorruptEvidence(
            format!("unexpected connector error after conservative mapping: {error}"),
        ),
    }
}

impl crate::sql::compiler::SqlStatisticsSnapshot for QueryStatisticsContext {
    fn collect_table_statistics(
        &self,
        database: &str,
        table: &crate::sql::planner::table::TableDef,
    ) -> (
        String,
        crate::sql::optimizer::stats_input::BaseTableStatistics,
    ) {
        collect_table_stats(self, database, table)
    }
}

/// Application adapter for the SQL catalog's provider-neutral materialization
/// seam.  The resulting binding carries the exact planning lease acquired for
/// metadata; SQL itself never names the Iceberg provider.
pub(crate) fn iceberg_table_binding_loader<'a>(
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Box<dyn QueryTableBindingLoader + 'a> {
    Box::new(IcebergTableBindingLoader {
        controls,
        connector_context,
    })
}

struct IcebergTableBindingLoader<'a> {
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl QueryTableBindingLoader for IcebergTableBindingLoader<'_> {
    fn load_strict_base_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        binding_id: crate::sql::binding::SqlTableBindingId,
    ) -> Result<QueryTableBinding, String> {
        let (base_table, snapshot_id) = parse_time_travel_overlay_identity(table)
            .map(|(base_table, snapshot_id)| (base_table, Some(snapshot_id)))
            .unwrap_or((table, None));
        let mut materialization = load_connector_table_materialization_with_lease(
            self.controls,
            self.connector_context.clone(),
            catalog,
            namespace,
            base_table,
        )?;
        if let Some(snapshot_id) = snapshot_id {
            materialization.read_selector =
                novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot_id);
        }
        connector_query_binding_from_materialization(
            materialization,
            catalog,
            namespace,
            table,
            binding_id,
        )
    }

    fn load_metadata_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        metadata_table_type: crate::sql::planner::table::SqlMetadataTableKind,
        binding_id: crate::sql::binding::SqlTableBindingId,
    ) -> Result<QueryTableBinding, String> {
        let alias = format!(
            "{table}${}",
            metadata_table_alias_suffix(metadata_table_type)
        );
        let materialization = load_connector_table_alias_materialization_with_lease(
            self.controls,
            self.connector_context.clone(),
            catalog,
            namespace,
            &alias,
        )?;
        let planner = crate::sql::planner::table::TableDef {
            name: table.to_string(),
            columns: materialization.columns,
            iceberg_row_lineage_metadata_columns: materialization.row_lineage_metadata_columns,
            source: ScanSource::Sql(SqlScanSource::new(
                binding_id,
                SqlTableIdentity {
                    catalog: catalog.to_string(),
                    namespace: namespace.to_string(),
                    table: table.to_string(),
                },
                SqlScanKind::Metadata {
                    kind: metadata_table_type,
                    version: SqlTableVersionSelector::Current,
                },
            )),
        };
        Ok(QueryTableBinding {
            resolved: ResolvedAnalyzerTable::from_planner(Some(catalog), namespace, planner),
            statistics_pin: materialization.statistics_pin.clone(),
            admission: QueryTableBindingAdmission::Exact(materialization.planning_lease.clone()),
            scan_materialization: Some(QueryScanMaterialization {
                table: materialization.read_table,
                schema: materialization.read_schema,
                selector: materialization.read_selector,
                statistics_pin: materialization.statistics_pin,
                planning_lease: materialization.planning_lease,
            }),
            mv_target_read: None,
            write_target_admission: None,
            frozen_snapshot_materializations: std::collections::BTreeMap::new(),
            admitted_change_scans: std::collections::BTreeMap::new(),
        })
    }
}

fn metadata_table_alias_suffix(
    kind: crate::sql::planner::table::SqlMetadataTableKind,
) -> &'static str {
    use crate::sql::planner::table::SqlMetadataTableKind;

    match kind {
        SqlMetadataTableKind::Snapshots => "SNAPSHOTS",
        SqlMetadataTableKind::History => "HISTORY",
        SqlMetadataTableKind::Refs => "REFS",
        SqlMetadataTableKind::Files => "FILES",
        SqlMetadataTableKind::Manifests => "MANIFESTS",
        SqlMetadataTableKind::Partitions => "PARTITIONS",
        SqlMetadataTableKind::LogicalIcebergMetadata => "LOGICAL_ICEBERG_METADATA",
    }
}

pub(crate) type QueryStatsPlan = crate::sql::compiler::SqlStatisticsPlan;

pub(crate) struct QueryStatsCollector {
    context: QueryStatisticsContext,
    next_stats_ref: u32,
    snapshot: QueryStatsSnapshot,
}

impl QueryStatsCollector {
    pub(crate) fn new(context: QueryStatisticsContext) -> Self {
        Self {
            context,
            next_stats_ref: 0,
            snapshot: QueryStatsSnapshot::empty(),
        }
    }

    pub(crate) fn collect(mut self, opt_expr: &mut OptExpr) -> QueryStatsPlan {
        self.walk(opt_expr);
        let mut plan = QueryStatsPlan::empty();
        plan.snapshot = self.snapshot;
        plan.set_next_stats_ref(self.next_stats_ref);
        plan
    }

    fn walk(&mut self, expr: &mut OptExpr) {
        if let Operator::LogicalScan(scan) = &mut expr.op {
            let stats_ref = StatsRef::new(self.next_stats_ref);
            self.next_stats_ref += 1;
            scan.stats_ref = Some(stats_ref);

            let (label, stats) = self.collect_scan(scan);
            self.snapshot.insert(stats_ref, label, stats);
        }

        for child in &mut expr.children {
            self.walk(child);
        }
    }

    fn collect_scan(
        &self,
        scan: &crate::sql::optimizer::operator::ScanOp,
    ) -> (String, BaseTableStatistics) {
        collect_table_stats(&self.context, &scan.database, &scan.table)
    }
}

pub(super) fn collect_table_stats(
    context: &QueryStatisticsContext,
    database: &str,
    table_def: &crate::sql::planner::table::TableDef,
) -> (String, BaseTableStatistics) {
    let label = table_label(database, table_def);
    let ScanSource::Sql(source) = &table_def.source;
    let binding_id = source.binding;
    if context.bindings.is_none() {
        return (
            label,
            BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                "query table bindings are not available".to_string(),
            )),
        );
    }
    match context.snapshot.get(binding_id) {
        Ok(evidence) => (evidence.label.clone(), evidence.statistics.clone()),
        Err(SqlStatisticsFatalError::BindingMissing) => {
            let Some(bindings) = context.bindings.as_ref() else {
                return (
                    label,
                    BaseTableStatistics::missing(StatsMissingReason::CatalogLoadError(
                        SqlStatisticsFatalError::BindingMissing.to_string(),
                    )),
                );
            };
            let Some(resolver) = context.resolver.as_ref() else {
                return (
                    label,
                    BaseTableStatistics::missing(StatsMissingReason::CatalogLoadError(
                        SqlStatisticsFatalError::BindingMissing.to_string(),
                    )),
                );
            };
            let binding = match bindings.binding(binding_id) {
                Ok(binding) => binding,
                Err(error) => {
                    return (
                        label,
                        BaseTableStatistics::missing(StatsMissingReason::CatalogLoadError(error)),
                    );
                }
            };
            match project_binding_statistics(resolver, &binding) {
                Ok(statistics) => (binding.resolved.catalog.identity.fqn(), statistics),
                Err(error) => (
                    binding.resolved.catalog.identity.fqn(),
                    BaseTableStatistics::missing(StatsMissingReason::CatalogLoadError(
                        error.to_string(),
                    )),
                ),
            }
        }
        // The legacy collector cannot return a compiler error.  The canonical
        // kernel will consume this same immutable snapshot directly when its
        // scan token migration lands; preserve conservative behavior here
        // while retaining the typed failure in the snapshot for submission.
        Err(error) => (
            label,
            BaseTableStatistics::missing(StatsMissingReason::CatalogLoadError(error.to_string())),
        ),
    }
}

fn metric_request(
    columns: &[novarocks_catalog::schema::ColumnDef],
) -> Result<StatisticsMetricRequest, novarocks_spi::connector::ConnectorError> {
    let mut metrics = Vec::with_capacity(1 + columns.len() * 5);
    metrics.push(StatisticsMetric::RowCount);
    for column in columns {
        let column = Arc::<str>::from(column.name.as_str());
        metrics.extend([
            StatisticsMetric::NullCount {
                column: Arc::clone(&column),
            },
            StatisticsMetric::Minimum {
                column: Arc::clone(&column),
            },
            StatisticsMetric::Maximum {
                column: Arc::clone(&column),
            },
            StatisticsMetric::AverageSize {
                column: Arc::clone(&column),
            },
            StatisticsMetric::ThetaNdv { column },
        ]);
    }
    StatisticsMetricRequest::try_new(metrics)
}

fn evidence_to_base_statistics(
    evidence: &novarocks_spi::connector::StatisticsEvidence,
    columns: &[novarocks_catalog::schema::ColumnDef],
) -> BaseTableStatistics {
    if !UnifiedStatisticsResolver::optimizer_usable(evidence) {
        return BaseTableStatistics::missing(StatsMissingReason::CatalogLoadError(
            "connector evidence is not Full+Exact for the pinned table version".to_string(),
        ));
    }
    let source = match evidence.provenance {
        StatisticsProvenance::ProviderArtifact => StatsSource::IcebergPuffin,
        StatisticsProvenance::Manifest => StatsSource::IcebergManifest,
        StatisticsProvenance::VisibleRows | StatisticsProvenance::Provider(_) => {
            StatsSource::ConnectorEstimate
        }
    };
    let row_count = metric_u64(evidence.metrics.get(&StatisticsMetric::RowCount));
    let row_count_stat = row_count
        .map(|value| StatValue::known(value, Confidence::Exact, source))
        .unwrap_or_else(|| {
            StatValue::missing(StatsMissingReason::ColumnNotReported("row_count".into()))
        });
    let mut base_columns = std::collections::HashMap::new();
    for column in columns {
        let name = column.name.to_ascii_lowercase();
        let key = Arc::<str>::from(column.name.as_str());
        let missing = || StatsMissingReason::ColumnNotReported(name.clone());
        let null_count = metric_u64(evidence.metrics.get(&StatisticsMetric::NullCount {
            column: Arc::clone(&key),
        }));
        let nulls_fraction = match (null_count, row_count) {
            (Some(nulls), Some(rows)) if rows > 0 => {
                StatValue::known(nulls as f64 / rows as f64, Confidence::Exact, source)
            }
            (Some(0), Some(0)) => StatValue::known(0.0, Confidence::Exact, source),
            _ => StatValue::missing(missing()),
        };
        base_columns.insert(
            name.clone(),
            BaseColumnStatistics {
                nulls_fraction,
                average_row_size: metric_f64(
                    evidence.metrics.get(&StatisticsMetric::AverageSize {
                        column: Arc::clone(&key),
                    }),
                    None,
                )
                .map(|value| StatValue::known(value, Confidence::Exact, source))
                .unwrap_or_else(|| StatValue::missing(missing())),
                min_value: metric_f64(
                    evidence.metrics.get(&StatisticsMetric::Minimum {
                        column: Arc::clone(&key),
                    }),
                    Some(&column.data_type),
                )
                .map(|value| StatValue::known(value, Confidence::Exact, source))
                .unwrap_or_else(|| StatValue::missing(missing())),
                max_value: metric_f64(
                    evidence.metrics.get(&StatisticsMetric::Maximum {
                        column: Arc::clone(&key),
                    }),
                    Some(&column.data_type),
                )
                .map(|value| StatValue::known(value, Confidence::Exact, source))
                .unwrap_or_else(|| StatValue::missing(missing())),
                // Theta is a mergeable approximate sketch. It is useful to
                // persist and expose through the typed statistics contract,
                // but must not be relabelled as an exact optimizer NDV.
                // Keep normal optimizer fallback until the optimizer gains a
                // confidence-aware approximate NDV input.
                ndv: StatValue::missing(StatsMissingReason::ColumnNotReported(format!(
                    "approximate Theta NDV for `{name}` is not an exact optimizer statistic"
                ))),
            },
        );
    }
    BaseTableStatistics {
        row_count: row_count_stat,
        columns: base_columns,
        source,
    }
}

fn metric_u64(state: Option<&StatisticsMetricState>) -> Option<u64> {
    match state {
        Some(StatisticsMetricState::Available(StatisticsMetricValue::U64(value))) => Some(*value),
        Some(StatisticsMetricState::Available(StatisticsMetricValue::I64(value))) => {
            u64::try_from(*value).ok()
        }
        Some(StatisticsMetricState::Available(StatisticsMetricValue::F64(value)))
            if value.is_finite() && *value >= 0.0 && *value <= u64::MAX as f64 =>
        {
            Some(*value as u64)
        }
        _ => None,
    }
}

fn metric_f64(state: Option<&StatisticsMetricState>, data_type: Option<&DataType>) -> Option<f64> {
    let value = match state {
        Some(StatisticsMetricState::Available(StatisticsMetricValue::U64(value))) => *value as f64,
        Some(StatisticsMetricState::Available(StatisticsMetricValue::I64(value))) => *value as f64,
        Some(StatisticsMetricState::Available(StatisticsMetricValue::F64(value))) => *value,
        // The provider artifact keeps LARGEINT bounds as exact i128 bytes.
        // Optimizer cardinality estimation is currently f64-only, so make the
        // approximation explicit at this terminal heuristic boundary rather
        // than losing information in collection or persistence.
        Some(StatisticsMetricState::Available(StatisticsMetricValue::Bytes(value)))
            if matches!(data_type, Some(DataType::FixedSizeBinary(width)) if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
                && value.len()
                    == usize::try_from(novarocks_types::largeint::LARGEINT_BYTE_WIDTH).ok()? =>
        {
            novarocks_types::largeint::i128_from_be_bytes(value).ok()? as f64
        }
        _ => return None,
    };
    value.is_finite().then_some(value)
}

fn table_label(database: &str, table_def: &crate::sql::planner::table::TableDef) -> String {
    let ScanSource::Sql(source) = &table_def.source;
    let _ = database;
    format!(
        "{}.{}.{}",
        source.table.catalog, source.table.namespace, source.table.table
    )
}

#[cfg(test)]
mod unified_tests {
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType;
    use bytes::Bytes;
    use novarocks_catalog::schema::ColumnDef;
    use novarocks_spi::connector::{
        StatisticsAccuracy, StatisticsCoverage, StatisticsDataVersion, StatisticsEvidence,
        StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricState, StatisticsMetricValue,
    };

    use super::*;

    fn column(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }
    }

    #[test]
    fn spi5b_largeint_artifact_bound_is_projected_only_for_optimizer_estimation() {
        let state = StatisticsMetricState::Available(StatisticsMetricValue::Bytes(
            Bytes::copy_from_slice(&i128::MIN.to_be_bytes()),
        ));
        let data_type = DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH);

        assert_eq!(
            metric_f64(Some(&state), Some(&data_type)),
            Some(i128::MIN as f64)
        );
        assert_eq!(metric_f64(Some(&state), Some(&DataType::Binary)), None);
    }

    fn evidence(coverage: StatisticsCoverage, accuracy: StatisticsAccuracy) -> StatisticsEvidence {
        StatisticsEvidence {
            data_version: StatisticsDataVersion::try_new(Bytes::from_static(b"v1")).unwrap(),
            evidence_revision: StatisticsEvidenceRevision::try_new(Bytes::from_static(b"r1"))
                .unwrap(),
            coverage,
            accuracy,
            interval: None,
            provenance: StatisticsProvenance::ProviderArtifact,
            metrics: BTreeMap::from([
                (
                    StatisticsMetric::RowCount,
                    StatisticsMetricState::Available(StatisticsMetricValue::U64(10)),
                ),
                (
                    StatisticsMetric::NullCount {
                        column: Arc::from("k"),
                    },
                    StatisticsMetricState::Available(StatisticsMetricValue::U64(2)),
                ),
                (
                    StatisticsMetric::Minimum {
                        column: Arc::from("k"),
                    },
                    StatisticsMetricState::Available(StatisticsMetricValue::F64(1.0)),
                ),
                (
                    StatisticsMetric::Maximum {
                        column: Arc::from("k"),
                    },
                    StatisticsMetricState::Available(StatisticsMetricValue::F64(9.0)),
                ),
                (
                    StatisticsMetric::AverageSize {
                        column: Arc::from("k"),
                    },
                    StatisticsMetricState::Available(StatisticsMetricValue::F64(8.0)),
                ),
                (
                    StatisticsMetric::ThetaNdv {
                        column: Arc::from("k"),
                    },
                    StatisticsMetricState::Available(StatisticsMetricValue::F64(7.0)),
                ),
            ]),
        }
    }

    #[test]
    fn exact_full_evidence_maps_to_query_scoped_optimizer_values() {
        let base = evidence_to_base_statistics(
            &evidence(StatisticsCoverage::Full, StatisticsAccuracy::Exact),
            &[column("k")],
        );
        assert_eq!(base.row_count.known_value(), Some(&10));
        let column = base.columns.get("k").unwrap();
        assert_eq!(column.nulls_fraction.known_value(), Some(&0.2));
        assert!(column.ndv.known_value().is_none());
    }

    #[test]
    fn subset_or_approximate_evidence_falls_back() {
        for (coverage, accuracy) in [
            (StatisticsCoverage::Subset, StatisticsAccuracy::Exact),
            (StatisticsCoverage::Full, StatisticsAccuracy::Approximate),
        ] {
            let base = evidence_to_base_statistics(&evidence(coverage, accuracy), &[column("k")]);
            assert!(base.row_count.known_value().is_none());
        }
    }

    #[test]
    fn request_uses_stable_column_metric_names() {
        let request = metric_request(&[column("k")]).unwrap();
        assert_eq!(request.metrics().len(), 6);
        assert!(request.metrics().contains(&StatisticsMetric::ThetaNdv {
            column: Arc::from("k"),
        }));
    }

    #[test]
    fn sqlx1_resolution_time_travel_overlay_identity_is_canonical() {
        assert_eq!(
            parse_time_travel_overlay_identity("__sqlx1_tt_orders_42"),
            Some(("orders", 42))
        );
        assert_eq!(
            parse_time_travel_overlay_identity("__sqlx1_tt_sales_orders_-7"),
            Some(("sales_orders", -7))
        );
        assert_eq!(parse_time_travel_overlay_identity("orders"), None);
        assert_eq!(parse_time_travel_overlay_identity("__sqlx1_tt__bad"), None);
    }
}
