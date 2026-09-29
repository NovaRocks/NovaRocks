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

//! Query-scoped statistics input for the optimizer.
//!
//! No engine, connector, or catalog dependencies belong in this module.

#![allow(dead_code)]

use std::collections::HashMap;

use crate::binding::SqlTableBindingId;
use crate::optimizer::statistics::{Confidence, TableStatistics};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct StatsRef(u32);

impl StatsRef {
    pub(crate) fn new(value: u32) -> Self {
        Self(value)
    }

    pub(crate) fn as_u32(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StatsSource {
    IcebergManifest,
    IcebergPuffin,
    ManagedLakeMetadata,
    StarRocksTableMetadata,
    ConnectorEstimate,
    Derived,
    Fallback,
    TestFixture,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StatsMissingReason {
    NoCurrentSnapshot,
    NoDataFiles,
    ManifestMissingRowCount,
    StatsFileMissing,
    ConnectorUnsupported(String),
    CatalogLoadError(String),
    ColumnNotReported(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum StatValue<T> {
    Known {
        value: T,
        confidence: Confidence,
        source: StatsSource,
    },
    Missing {
        reason: StatsMissingReason,
    },
}

impl<T> StatValue<T> {
    pub(crate) fn known(value: T, confidence: Confidence, source: StatsSource) -> Self {
        Self::Known {
            value,
            confidence,
            source,
        }
    }

    pub(crate) fn missing(reason: StatsMissingReason) -> Self {
        Self::Missing { reason }
    }

    pub(crate) fn known_value(&self) -> Option<&T> {
        match self {
            Self::Known { value, .. } => Some(value),
            Self::Missing { .. } => None,
        }
    }

    pub(crate) fn confidence(&self) -> Confidence {
        match self {
            Self::Known { confidence, .. } => *confidence,
            Self::Missing { .. } => Confidence::Fallback,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BaseColumnStatistics {
    pub nulls_fraction: StatValue<f64>,
    pub average_row_size: StatValue<f64>,
    pub min_value: StatValue<f64>,
    pub max_value: StatValue<f64>,
    pub ndv: StatValue<f64>,
}

impl BaseColumnStatistics {
    pub(crate) fn missing(column: &str) -> Self {
        let reason = StatsMissingReason::ColumnNotReported(column.to_ascii_lowercase());
        Self {
            nulls_fraction: StatValue::missing(reason.clone()),
            average_row_size: StatValue::missing(reason.clone()),
            min_value: StatValue::missing(reason.clone()),
            max_value: StatValue::missing(reason.clone()),
            ndv: StatValue::missing(reason),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BaseTableStatistics {
    pub row_count: StatValue<u64>,
    pub columns: HashMap<String, BaseColumnStatistics>,
    pub source: StatsSource,
}

/// Typed failures for evidence that claims to describe a query-local binding
/// but cannot be proven to do so.  These are deliberately distinct from
/// incomplete statistics: absence remains conservative optimizer input,
/// whereas a mismatched or malformed fact must stop planning before a stale
/// connector generation can be used.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlStatisticsFatalError {
    BindingMissing,
    OwnerMismatch,
    IncarnationMismatch,
    DataVersionMismatch,
    CorruptEvidence(String),
}

impl std::fmt::Display for SqlStatisticsFatalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BindingMissing => formatter.write_str("SQL statistics binding is missing"),
            Self::OwnerMismatch => {
                formatter.write_str("SQL statistics owner does not match binding")
            }
            Self::IncarnationMismatch => {
                formatter.write_str("SQL statistics incarnation does not match binding")
            }
            Self::DataVersionMismatch => {
                formatter.write_str("SQL statistics data version does not match binding")
            }
            Self::CorruptEvidence(message) => {
                write!(formatter, "SQL statistics evidence is corrupt: {message}")
            }
        }
    }
}

/// One immutable SQL-facing statistics observation.  The application has
/// already validated and projected any connector evidence before constructing
/// this value; it intentionally contains no provider authority or bytes.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlTableStatisticsEvidence {
    pub(crate) label: String,
    pub(crate) statistics: BaseTableStatistics,
}

/// Query-scoped SQL statistics input keyed by the exact table binding token.
/// Both successes and typed fatal failures are memoized before compilation, so
/// optimizer phases cannot refresh a connector or observe another request.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SqlStatisticsSnapshot {
    entries:
        HashMap<SqlTableBindingId, Result<SqlTableStatisticsEvidence, SqlStatisticsFatalError>>,
}

impl SqlStatisticsSnapshot {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn insert(
        &mut self,
        binding: SqlTableBindingId,
        evidence: SqlTableStatisticsEvidence,
    ) {
        self.entries.insert(binding, Ok(evidence));
    }

    pub(crate) fn insert_fatal(
        &mut self,
        binding: SqlTableBindingId,
        error: SqlStatisticsFatalError,
    ) {
        self.entries.insert(binding, Err(error));
    }

    pub(crate) fn get(
        &self,
        binding: SqlTableBindingId,
    ) -> Result<&SqlTableStatisticsEvidence, SqlStatisticsFatalError> {
        self.entries
            .get(&binding)
            .ok_or(SqlStatisticsFatalError::BindingMissing)?
            .as_ref()
            .map_err(Clone::clone)
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

impl BaseTableStatistics {
    pub(crate) fn missing(reason: StatsMissingReason) -> Self {
        Self {
            row_count: StatValue::missing(reason),
            columns: HashMap::new(),
            source: StatsSource::Fallback,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryStatsEntry {
    pub label: String,
    pub stats: BaseTableStatistics,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct QueryStatsSnapshot {
    entries: HashMap<StatsRef, QueryStatsEntry>,
}

/// Frozen base-table statistics used by the optimizer, displayed by the plan.
pub(crate) const TABLE_STATISTICS_ANNOTATION_KEY: &str = "optimizer.table_statistics";

impl QueryStatsSnapshot {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn insert(
        &mut self,
        stats_ref: StatsRef,
        label: impl Into<String>,
        stats: BaseTableStatistics,
    ) {
        self.entries.insert(
            stats_ref,
            QueryStatsEntry {
                label: label.into(),
                stats,
            },
        );
    }

    pub(crate) fn get(&self, stats_ref: StatsRef) -> Option<&BaseTableStatistics> {
        self.entries.get(&stats_ref).map(|entry| &entry.stats)
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Keep precisely the facts optimization consumed, without consulting a
    /// catalog or converting derived operator estimates into base-table facts.
    pub(crate) fn annotate_final_plan(&self, builder: &mut novarocks_physical_plan::PlanBuilder) {
        for row in self.display_rows() {
            builder.add_annotation(novarocks_physical_plan::PlanAnnotation {
                subject: novarocks_physical_plan::AnnotationSubject::Plan,
                key: TABLE_STATISTICS_ANNOTATION_KEY.into(),
                value: row.into_boxed_str(),
            });
        }
    }

    pub(crate) fn display_rows(&self) -> Vec<String> {
        let mut entries: Vec<_> = self.entries.iter().collect();
        entries.sort_by_key(|(stats_ref, _)| stats_ref.as_u32());

        entries
            .into_iter()
            .map(|(stats_ref, entry)| match &entry.stats.row_count {
                StatValue::Known {
                    value,
                    confidence,
                    source,
                } => format!(
                    "TABLE STATS ref={} table={} rows={} confidence={:?} source={:?}",
                    stats_ref.as_u32(),
                    entry.label,
                    value,
                    confidence,
                    source
                ),
                StatValue::Missing { reason } => format!(
                    "TABLE STATS ref={} table={} rows=missing reason={:?}",
                    stats_ref.as_u32(),
                    entry.label,
                    reason
                ),
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OptimizerStatsInput {
    query_stats: QueryStatsSnapshot,
    // Transitional bridge for legacy rewrite/test callers that have not been
    // moved to query-scoped StatsRef binding yet. Base scan row counts must
    // come from `query_stats`, never from this table-name map.
    test_table_statistics: Option<HashMap<String, TableStatistics>>,
}

impl OptimizerStatsInput {
    pub(crate) fn from_query_stats(query_stats: &QueryStatsSnapshot) -> Self {
        Self {
            query_stats: query_stats.clone(),
            test_table_statistics: None,
        }
    }

    pub(crate) fn from_test_table_statistics(
        table_stats: &HashMap<String, TableStatistics>,
    ) -> Self {
        Self {
            query_stats: QueryStatsSnapshot::empty(),
            test_table_statistics: Some(table_stats.clone()),
        }
    }

    pub(crate) fn query_stats(&self) -> &QueryStatsSnapshot {
        &self.query_stats
    }

    pub(crate) fn test_table_statistics(&self) -> Option<&HashMap<String, TableStatistics>> {
        self.test_table_statistics.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use super::*;
    use crate::optimizer::statistics::TableStatistics;

    fn binding(ordinal: u32) -> SqlTableBindingId {
        SqlTableBindingId::new(
            crate::binding::SqlTableBindingScopeId::new(
                NonZeroU64::new(11).expect("nonzero scope"),
            ),
            NonZeroU32::new(ordinal).expect("nonzero ordinal"),
        )
    }

    #[test]
    fn sqlx2_resolution_snapshot_keeps_missing_evidence_conservative() {
        let mut snapshot = SqlStatisticsSnapshot::empty();
        snapshot.insert(
            binding(1),
            SqlTableStatisticsEvidence {
                label: "ice.db.orders".to_string(),
                statistics: BaseTableStatistics::missing(StatsMissingReason::NoDataFiles),
            },
        );

        let evidence = snapshot.get(binding(1)).expect("snapshot evidence");
        assert!(evidence.statistics.row_count.known_value().is_none());
        assert_eq!(snapshot.len(), 1);
    }

    #[test]
    fn sqlx2_resolution_snapshot_preserves_typed_fatal_binding_failures() {
        let mut snapshot = SqlStatisticsSnapshot::empty();
        snapshot.insert_fatal(binding(2), SqlStatisticsFatalError::DataVersionMismatch);
        snapshot.insert_fatal(
            binding(3),
            SqlStatisticsFatalError::CorruptEvidence("invalid bounds".to_string()),
        );

        assert_eq!(
            snapshot.get(binding(2)).unwrap_err(),
            SqlStatisticsFatalError::DataVersionMismatch
        );
        assert_eq!(
            snapshot.get(binding(3)).unwrap_err(),
            SqlStatisticsFatalError::CorruptEvidence("invalid bounds".to_string())
        );
        assert_eq!(
            snapshot.get(binding(4)).unwrap_err(),
            SqlStatisticsFatalError::BindingMissing
        );
    }

    #[test]
    fn display_rows_sort_by_numeric_ref() {
        let mut snapshot = QueryStatsSnapshot::empty();
        snapshot.insert(
            StatsRef::new(10),
            "ten",
            BaseTableStatistics::missing(StatsMissingReason::NoDataFiles),
        );
        snapshot.insert(
            StatsRef::new(2),
            "two",
            BaseTableStatistics {
                row_count: StatValue::known(
                    7,
                    crate::optimizer::statistics::Confidence::Exact,
                    StatsSource::IcebergManifest,
                ),
                columns: std::collections::HashMap::new(),
                source: StatsSource::IcebergManifest,
            },
        );

        assert_eq!(
            snapshot.display_rows(),
            vec![
                "TABLE STATS ref=2 table=two rows=7 confidence=Exact source=IcebergManifest"
                    .to_string(),
                "TABLE STATS ref=10 table=ten rows=missing reason=NoDataFiles".to_string(),
            ]
        );
    }

    #[test]
    fn get_returns_base_table_statistics() {
        let mut snapshot = QueryStatsSnapshot::empty();
        snapshot.insert(
            StatsRef::new(1),
            "orders",
            BaseTableStatistics {
                row_count: StatValue::known(42, Confidence::Exact, StatsSource::IcebergManifest),
                columns: std::collections::HashMap::new(),
                source: StatsSource::IcebergManifest,
            },
        );

        assert_eq!(
            snapshot
                .get(StatsRef::new(1))
                .unwrap()
                .row_count
                .known_value(),
            Some(&42)
        );
    }

    #[test]
    fn missing_confidence_falls_back() {
        let value: StatValue<u64> = StatValue::missing(StatsMissingReason::NoCurrentSnapshot);

        assert_eq!(value.confidence(), Confidence::Fallback);
    }

    #[test]
    fn connector_unsupported_preserves_reason() {
        let value: StatValue<u64> =
            StatValue::missing(StatsMissingReason::ConnectorUnsupported("jdbc".to_string()));

        assert_eq!(
            value,
            StatValue::Missing {
                reason: StatsMissingReason::ConnectorUnsupported("jdbc".to_string()),
            }
        );
    }

    #[test]
    fn query_stats_input_constructor_has_no_legacy_map() {
        let mut snapshot = QueryStatsSnapshot::empty();
        snapshot.insert(
            StatsRef::new(7),
            "orders",
            BaseTableStatistics::missing(StatsMissingReason::NoDataFiles),
        );

        let input = OptimizerStatsInput::from_query_stats(&snapshot);

        assert_eq!(input.query_stats().len(), 1);
        assert!(input.test_table_statistics().is_none());
    }

    #[test]
    fn legacy_stats_input_constructor_preserves_table_entry() {
        let mut table_stats = HashMap::new();
        table_stats.insert(
            "orders".to_string(),
            TableStatistics {
                row_count: 42,
                column_stats: HashMap::new(),
            },
        );

        let input = OptimizerStatsInput::from_test_table_statistics(&table_stats);

        assert_eq!(input.query_stats().len(), 0);
        assert_eq!(
            input
                .test_table_statistics()
                .unwrap()
                .get("orders")
                .unwrap()
                .row_count,
            42
        );
    }
}
