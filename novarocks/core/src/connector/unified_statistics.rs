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

//! Generation-fenced, query-safe connector statistics resolution.
//!
//! This is deliberately a read/cache boundary.  It owns neither catalog
//! clients nor global optimizer state: callers hand it a table handle and the
//! exact data-version returned by the same table-resolution operation that
//! planned the scan.  The resulting evidence is immutable and cacheable only
//! by its explicit evidence revision.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorRequestContext,
    ConnectorStatistics, ConnectorTableHandle, StatisticsAccuracy, StatisticsCoverage,
    StatisticsDataVersion, StatisticsEvidence, StatisticsEvidenceRevision, StatisticsMetric,
    StatisticsMetricRequest, StatisticsReadRequest,
};

/// Application-side classification of a statistics read that cannot be
/// represented as conservative missing evidence.  The SQL projection maps
/// these values into its own typed fatal vocabulary without importing a
/// connector error or capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StatisticsResolutionFailure {
    OwnerMismatch,
    IncarnationMismatch,
    DataVersionMismatch,
    CorruptEvidence(String),
    Connector(ConnectorError),
}

#[derive(Clone)]
pub(crate) struct ResolvedStatisticsTable {
    pub table: ConnectorTableHandle,
    pub data_version: StatisticsDataVersion,
    pub incarnation: ConnectorInstanceIncarnation,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ResolvedCacheKey {
    instance_id: ConnectorInstanceId,
    incarnation: ConnectorInstanceIncarnation,
    table_payload: Bytes,
    data_version: StatisticsDataVersion,
    metrics: Vec<StatisticsMetric>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ArtifactCacheKey {
    instance_id: ConnectorInstanceId,
    incarnation: ConnectorInstanceIncarnation,
    table_payload: Bytes,
    data_version: StatisticsDataVersion,
    evidence_revision: StatisticsEvidenceRevision,
    metrics: Vec<StatisticsMetric>,
}

/// A small in-process cache only for immutable, revision-addressed response
/// values. The connector read still runs for every query because a metadata-only
/// statistics publication can change the evidence revision without changing
/// the table data version. A query takes a cloned `StatisticsEvidence` into its
/// own `QueryStatsSnapshot` rather than reading this map during optimization.
#[derive(Default)]
pub struct UnifiedStatisticsResolver {
    artifacts: Mutex<HashMap<ArtifactCacheKey, Arc<StatisticsEvidence>>>,
}

impl UnifiedStatisticsResolver {
    pub(crate) fn resolve(
        &self,
        table: &ResolvedStatisticsTable,
        statistics: &dyn ConnectorStatistics,
        metrics: StatisticsMetricRequest,
        context: ConnectorRequestContext,
    ) -> Result<Arc<StatisticsEvidence>, StatisticsResolutionFailure> {
        if statistics.descriptor().instance_id != *table.table.owner() {
            return Err(StatisticsResolutionFailure::OwnerMismatch);
        }
        if statistics.incarnation() != table.incarnation {
            return Err(StatisticsResolutionFailure::IncarnationMismatch);
        }
        let key = ResolvedCacheKey {
            instance_id: table.table.owner().clone(),
            incarnation: table.incarnation,
            table_payload: table.table.payload().clone(),
            data_version: table.data_version.clone(),
            metrics: metrics.metrics().to_vec(),
        };
        let evidence = statistics
            .read_statistics(StatisticsReadRequest {
                table: table.table.clone(),
                data_version: table.data_version.clone(),
                metrics,
                context,
            })
            .map_err(|error| match error.kind() {
                novarocks_spi::connector::ConnectorErrorKind::CorruptData => {
                    StatisticsResolutionFailure::CorruptEvidence(error.to_string())
                }
                _ => StatisticsResolutionFailure::Connector(error),
            })?;
        if evidence.data_version != table.data_version {
            return Err(StatisticsResolutionFailure::DataVersionMismatch);
        }
        let artifact_key = ArtifactCacheKey {
            instance_id: key.instance_id.clone(),
            incarnation: key.incarnation,
            table_payload: key.table_payload.clone(),
            data_version: key.data_version.clone(),
            evidence_revision: evidence.evidence_revision.clone(),
            metrics: key.metrics.clone(),
        };
        let evidence = {
            let mut artifacts = self
                .artifacts
                .lock()
                .expect("unified statistics artifact cache lock");
            artifacts
                .entry(artifact_key)
                .or_insert_with(|| Arc::new(evidence))
                .clone()
        };
        Ok(evidence)
    }

    /// Optimizer input is allowed only when the provider proved the evidence
    /// covers the pinned table and every value is exact. Subset/Superset and
    /// approximate responses intentionally force normal missing-stat fallback.
    pub(crate) fn optimizer_usable(evidence: &StatisticsEvidence) -> bool {
        evidence.coverage == StatisticsCoverage::Full
            && evidence.accuracy == StatisticsAccuracy::Exact
    }

    #[cfg(test)]
    pub(crate) fn cache_sizes(&self) -> (usize, usize) {
        (
            0,
            self.artifacts
                .lock()
                .expect("unified statistics artifact cache lock")
                .len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use novarocks_spi::connector::{StatisticsEvidenceRevision, StatisticsProvenance};

    use super::*;

    fn evidence(coverage: StatisticsCoverage, accuracy: StatisticsAccuracy) -> StatisticsEvidence {
        StatisticsEvidence {
            data_version: StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1"))
                .expect("bounded data version"),
            evidence_revision: StatisticsEvidenceRevision::try_new(Bytes::from_static(b"rev-1"))
                .expect("bounded evidence revision"),
            coverage,
            accuracy,
            interval: None,
            provenance: StatisticsProvenance::Manifest,
            metrics: BTreeMap::new(),
        }
    }

    #[test]
    fn subset_or_approximate_evidence_cannot_upgrade_optimizer_input() {
        assert!(!UnifiedStatisticsResolver::optimizer_usable(&evidence(
            StatisticsCoverage::Subset,
            StatisticsAccuracy::Exact,
        )));
        assert!(!UnifiedStatisticsResolver::optimizer_usable(&evidence(
            StatisticsCoverage::Full,
            StatisticsAccuracy::Approximate,
        )));
        assert!(UnifiedStatisticsResolver::optimizer_usable(&evidence(
            StatisticsCoverage::Full,
            StatisticsAccuracy::Exact,
        )));
    }
}
