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

use crate::sql::optimizer::statistics::{Confidence, TableStatistics};

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
    use super::*;
    use crate::sql::optimizer::statistics::TableStatistics;

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
                    crate::sql::optimizer::statistics::Confidence::Exact,
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
