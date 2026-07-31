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

use novarocks_spi::connector::{
    ConnectorControlRegistry, StatisticsMetric, StatisticsMetricRequest, StatisticsMetricState,
    StatisticsMetricValue, StatisticsProvenance,
};

use crate::connector::unified_statistics::{ResolvedStatisticsTable, UnifiedStatisticsResolver};
use crate::sql::catalog::provider::QueryStatisticsPins;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::statistics::Confidence;
use crate::sql::optimizer::stats_input::{
    BaseColumnStatistics, BaseTableStatistics, QueryStatsSnapshot, StatValue, StatsMissingReason,
    StatsRef, StatsSource,
};
use crate::sql::planner::table::ScanSource;

#[derive(Clone, Default)]
/// Query-scoped handles for the one unified statistics resolver.  This is not
/// a provider registry: absent pins intentionally produce missing statistics
/// rather than a second latest-resolution path.
pub(crate) struct QueryStatisticsContext {
    connector_control: Option<Arc<dyn ConnectorControlRegistry>>,
    resolver: Option<Arc<UnifiedStatisticsResolver>>,
    pins: Option<QueryStatisticsPins>,
}

impl QueryStatisticsContext {
    pub(crate) fn none() -> Self {
        Self::default()
    }

    pub(crate) fn unavailable() -> Self {
        Self::none()
    }

    pub(crate) fn from_standalone_state_with_pins(
        state: &Arc<super::StandaloneState>,
        pins: QueryStatisticsPins,
    ) -> Self {
        Self {
            connector_control: Some(Arc::clone(&state.connector_control)),
            resolver: Some(Arc::clone(&state.unified_statistics)),
            pins: Some(pins),
        }
    }

    pub(crate) fn from_optional_state_with_pins(
        state: Option<&Arc<super::StandaloneState>>,
        pins: Option<QueryStatisticsPins>,
    ) -> Self {
        match (state, pins) {
            (Some(state), Some(pins)) => Self::from_standalone_state_with_pins(state, pins),
            // A caller without resolution pins must not resolve `latest` a
            // second time. Keep normal missing-statistics fallback instead.
            (Some(_), None) => Self::none(),
            (None, _) => Self::none(),
        }
    }
}

pub(crate) struct QueryStatsPlan {
    pub snapshot: QueryStatsSnapshot,
    next_stats_ref: u32,
}

impl QueryStatsPlan {
    fn new(snapshot: QueryStatsSnapshot, next_stats_ref: u32) -> Self {
        Self {
            snapshot,
            next_stats_ref,
        }
    }

    pub(crate) fn add_stats(
        &mut self,
        label: impl Into<String>,
        stats: BaseTableStatistics,
    ) -> StatsRef {
        let stats_ref = StatsRef::new(self.next_stats_ref);
        self.next_stats_ref += 1;
        self.snapshot.insert(stats_ref, label, stats);
        stats_ref
    }
}

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
        QueryStatsPlan::new(self.snapshot, self.next_stats_ref)
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
    let ScanSource::IcebergDataFiles { table, .. } = &table_def.source else {
        return (
            label,
            BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                "scan source does not expose connector statistics".to_string(),
            )),
        );
    };
    let Some(control) = context.connector_control.as_deref() else {
        return (
            label,
            BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                "connector statistics resolver is not available".to_string(),
            )),
        );
    };
    let Some(resolver) = context.resolver.as_deref() else {
        return (
            label,
            BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                "unified statistics cache is not available".to_string(),
            )),
        );
    };
    let Some(pins) = context.pins.as_ref() else {
        return (
            label,
            BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                "query statistics pins are not available".to_string(),
            )),
        );
    };
    let pin = pins
        .lock()
        .expect("query statistics pin lock")
        .get(&(
            table.catalog.to_ascii_lowercase(),
            table.namespace.to_ascii_lowercase(),
            table.table.to_ascii_lowercase(),
        ))
        .cloned();
    let Some(pin) = pin else {
        return (
            label,
            BaseTableStatistics::missing(StatsMissingReason::CatalogLoadError(
                "table resolution did not retain a statistics data-version pin".to_string(),
            )),
        );
    };
    let metrics = metric_request(&table_def.columns).map_err(|error| error.to_string());
    let context = crate::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    );
    let stats = metrics
        .and_then(|metrics| {
            context.and_then(|context| {
                resolver
                    .resolve(
                        control,
                        &ResolvedStatisticsTable {
                            table: pin.table.clone(),
                            data_version: pin.data_version.clone(),
                        },
                        metrics,
                        context,
                    )
                    .map_err(|error| error.to_string())
            })
        })
        .map(|evidence| evidence_to_base_statistics(&evidence, &table_def.columns))
        .unwrap_or_else(|error| {
            BaseTableStatistics::missing(StatsMissingReason::CatalogLoadError(error))
        });
    (label, stats)
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
                average_row_size: metric_f64(evidence.metrics.get(
                    &StatisticsMetric::AverageSize {
                        column: Arc::clone(&key),
                    },
                ))
                .map(|value| StatValue::known(value, Confidence::Exact, source))
                .unwrap_or_else(|| StatValue::missing(missing())),
                min_value: metric_f64(evidence.metrics.get(&StatisticsMetric::Minimum {
                    column: Arc::clone(&key),
                }))
                .map(|value| StatValue::known(value, Confidence::Exact, source))
                .unwrap_or_else(|| StatValue::missing(missing())),
                max_value: metric_f64(evidence.metrics.get(&StatisticsMetric::Maximum {
                    column: Arc::clone(&key),
                }))
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

fn metric_f64(state: Option<&StatisticsMetricState>) -> Option<f64> {
    let value = match state {
        Some(StatisticsMetricState::Available(StatisticsMetricValue::U64(value))) => *value as f64,
        Some(StatisticsMetricState::Available(StatisticsMetricValue::I64(value))) => *value as f64,
        Some(StatisticsMetricState::Available(StatisticsMetricValue::F64(value))) => *value,
        _ => return None,
    };
    value.is_finite().then_some(value)
}

fn table_label(database: &str, table_def: &crate::sql::planner::table::TableDef) -> String {
    match &table_def.source {
        ScanSource::IcebergDataFiles { table, .. }
        | ScanSource::IcebergVersionTable { table, .. }
        | ScanSource::IcebergDeltaTable { table, .. } => {
            format!("{}.{}.{}", table.catalog, table.namespace, table.table)
        }
        _ => format!("{}.{}", database, table_def.name),
    }
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
}
