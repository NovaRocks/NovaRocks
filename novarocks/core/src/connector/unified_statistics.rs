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
    ConnectorError, ConnectorInstanceId, ConnectorRequestContext, ConnectorStatisticsResolver,
    ConnectorTableHandle, StatisticsAccuracy, StatisticsCoverage, StatisticsDataVersion,
    StatisticsEvidence, StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricRequest,
    StatisticsReadRequest,
};

#[derive(Clone)]
pub(crate) struct ResolvedStatisticsTable {
    pub table: ConnectorTableHandle,
    pub data_version: StatisticsDataVersion,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ResolvedCacheKey {
    instance_id: ConnectorInstanceId,
    table_payload: Bytes,
    data_version: StatisticsDataVersion,
    metrics: Vec<StatisticsMetric>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ArtifactCacheKey {
    instance_id: ConnectorInstanceId,
    table_payload: Bytes,
    data_version: StatisticsDataVersion,
    evidence_revision: StatisticsEvidenceRevision,
    metrics: Vec<StatisticsMetric>,
}

/// A small in-process cache only for immutable response values. It never
/// stores connector leases, runtime handles, collection artifacts, or StateStore
/// values. A query takes a cloned `StatisticsEvidence` into its own
/// `QueryStatsSnapshot` rather than reading these maps during optimization.
#[derive(Default)]
pub(crate) struct UnifiedStatisticsResolver {
    resolved: Mutex<HashMap<ResolvedCacheKey, Arc<StatisticsEvidence>>>,
    artifacts: Mutex<HashMap<ArtifactCacheKey, Arc<StatisticsEvidence>>>,
}

impl UnifiedStatisticsResolver {
    pub(crate) fn resolve(
        &self,
        resolver: &dyn ConnectorStatisticsResolver,
        table: &ResolvedStatisticsTable,
        metrics: StatisticsMetricRequest,
        context: ConnectorRequestContext,
    ) -> Result<Arc<StatisticsEvidence>, ConnectorError> {
        let key = ResolvedCacheKey {
            instance_id: table.table.owner().clone(),
            table_payload: table.table.payload().clone(),
            data_version: table.data_version.clone(),
            metrics: metrics.metrics().to_vec(),
        };
        if let Some(evidence) = self
            .resolved
            .lock()
            .expect("unified statistics resolved cache lock")
            .get(&key)
            .cloned()
        {
            return Ok(evidence);
        }

        let lease = resolver.acquire_current_statistics(table.table.owner())?;
        let evidence = lease.read(StatisticsReadRequest {
            table: table.table.clone(),
            data_version: table.data_version.clone(),
            metrics,
            context,
        })?;
        if evidence.data_version != table.data_version {
            return Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                "connector statistics evidence did not echo the resolved table data version",
            ));
        }
        let artifact_key = ArtifactCacheKey {
            instance_id: key.instance_id.clone(),
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
        self.resolved
            .lock()
            .expect("unified statistics resolved cache lock")
            .insert(key, Arc::clone(&evidence));
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
            self.resolved
                .lock()
                .expect("unified statistics resolved cache lock")
                .len(),
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
