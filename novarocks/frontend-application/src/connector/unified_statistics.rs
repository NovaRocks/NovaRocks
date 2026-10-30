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
//!
//! A mismatched evidence-level data version is still fatal here: the provider
//! answered about a different table state than the one being planned.  Whether
//! an individual *metric* may be used is not decided at this boundary — that is
//! per metric, via `StatisticsMetricObservation::describes_queried_rows`, so that one
//! degraded metric cannot disqualify the rest of the answer.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorInstanceId, ConnectorRequestContext, ConnectorStatistics,
    ConnectorTableHandle, ProviderBindingEpoch, StatisticsDataVersion, StatisticsEvidence,
    StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricRequest, StatisticsReadRequest,
};

/// Application-side classification of a statistics read that cannot be
/// represented as conservative missing evidence.  The SQL projection maps
/// these values into its own typed fatal vocabulary without importing a
/// connector error or capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsResolutionFailure {
    OwnerMismatch,
    IncarnationMismatch,
    DataVersionMismatch,
    CorruptEvidence(String),
    Connector(ConnectorError),
}

#[derive(Clone)]
pub struct ResolvedStatisticsTable {
    pub table: ConnectorTableHandle,
    pub data_version: StatisticsDataVersion,
    pub incarnation: ProviderBindingEpoch,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ResolvedCacheKey {
    instance_id: ConnectorInstanceId,
    incarnation: ProviderBindingEpoch,
    table_payload: Bytes,
    data_version: StatisticsDataVersion,
    metrics: Vec<StatisticsMetric>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ArtifactCacheKey {
    instance_id: ConnectorInstanceId,
    incarnation: ProviderBindingEpoch,
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
    pub async fn resolve_async(
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
            .read_statistics_async(StatisticsReadRequest {
                // No negotiation has happened at this point, so this asks
                // about the table rather than about a narrowed read.
                narrowed_read: None,
                table: table.table.clone(),
                data_version: table.data_version.clone(),
                metrics,
                context,
            })
            .await
            .map_err(|error| match error.kind() {
                novarocks_spi::connector::ConnectorErrorKind::CorruptData => {
                    StatisticsResolutionFailure::CorruptEvidence(error.to_string())
                }
                _ => StatisticsResolutionFailure::Connector(error),
            })?;
        self.retain_evidence(table, key, evidence)
    }

    fn retain_evidence(
        &self,
        table: &ResolvedStatisticsTable,
        key: ResolvedCacheKey,
        evidence: StatisticsEvidence,
    ) -> Result<Arc<StatisticsEvidence>, StatisticsResolutionFailure> {
        if *evidence.data_version() != table.data_version {
            return Err(StatisticsResolutionFailure::DataVersionMismatch);
        }
        let artifact_key = ArtifactCacheKey {
            instance_id: key.instance_id.clone(),
            incarnation: key.incarnation,
            table_payload: key.table_payload.clone(),
            data_version: key.data_version.clone(),
            evidence_revision: evidence.evidence_revision().clone(),
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
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        StatisticsEvidenceRevision, StatisticsMetricState, StatisticsReader, StatisticsRowCoverage,
    };

    use super::*;

    fn data_version(token: &'static [u8]) -> StatisticsDataVersion {
        StatisticsDataVersion::try_new(Bytes::from_static(token)).expect("bounded data version")
    }

    fn evidence_with(
        metrics: BTreeMap<StatisticsMetric, StatisticsMetricState>,
    ) -> StatisticsEvidence {
        StatisticsEvidence::try_new(
            data_version(b"data-v1"),
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"rev-1"))
                .expect("bounded evidence revision"),
            StatisticsRowCoverage::AllVisibleRows,
            metrics,
        )
        .expect("evidence")
    }

    fn evidence() -> StatisticsEvidence {
        evidence_with(BTreeMap::new())
    }

    struct TestStatistics {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        evidence: StatisticsEvidence,
    }

    fn descriptor(instance: &str) -> ConnectorInstanceDescriptor {
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
            instance_id: ConnectorInstanceId::parse(instance).expect("instance id"),
        }
    }

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1,
            1,
        )
        .expect("request context")
    }

    fn metric_request() -> StatisticsMetricRequest {
        StatisticsMetricRequest::try_new(vec![StatisticsMetric::RowCount]).expect("metric request")
    }

    fn resolved_table(
        instance: &str,
        incarnation: ProviderBindingEpoch,
    ) -> ResolvedStatisticsTable {
        ResolvedStatisticsTable {
            table: ConnectorTableHandle::try_new(
                ConnectorInstanceId::parse(instance).expect("instance id"),
                Bytes::from_static(b"table"),
            )
            .expect("table handle"),
            data_version: StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1"))
                .expect("data version"),
            incarnation,
        }
    }

    #[tokio::test]
    async fn resolver_fails_closed_for_owner_incarnation_and_data_version_mismatch() {
        let resolver = UnifiedStatisticsResolver::default();
        let asynchronous = |inner| AsyncOnlyStatistics {
            inner,
            gate: Mutex::new(None),
            calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let incarnation = ProviderBindingEpoch::from_bytes([1; 16]);
        let table = resolved_table("ice.main", incarnation);

        let owner_mismatch = asynchronous(TestStatistics {
            descriptor: descriptor("ice.foreign"),
            incarnation,
            evidence: evidence(),
        });
        assert_eq!(
            resolver
                .resolve_async(&table, &owner_mismatch, metric_request(), request_context())
                .await,
            Err(StatisticsResolutionFailure::OwnerMismatch)
        );
        assert_eq!(
            owner_mismatch
                .calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );

        let incarnation_mismatch = asynchronous(TestStatistics {
            descriptor: descriptor("ice.main"),
            incarnation: ProviderBindingEpoch::from_bytes([2; 16]),
            evidence: evidence(),
        });
        assert_eq!(
            resolver
                .resolve_async(
                    &table,
                    &incarnation_mismatch,
                    metric_request(),
                    request_context(),
                )
                .await,
            Err(StatisticsResolutionFailure::IncarnationMismatch)
        );
        assert_eq!(
            incarnation_mismatch
                .calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );

        let foreign_evidence = StatisticsEvidence::try_new(
            data_version(b"data-v2"),
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"rev-1"))
                .expect("bounded evidence revision"),
            StatisticsRowCoverage::AllVisibleRows,
            BTreeMap::new(),
        )
        .expect("foreign evidence");
        let data_version_mismatch = asynchronous(TestStatistics {
            descriptor: descriptor("ice.main"),
            incarnation,
            evidence: foreign_evidence,
        });
        assert_eq!(
            resolver
                .resolve_async(
                    &table,
                    &data_version_mismatch,
                    metric_request(),
                    request_context(),
                )
                .await,
            Err(StatisticsResolutionFailure::DataVersionMismatch)
        );
        assert_eq!(
            data_version_mismatch
                .calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert!(resolver.artifacts.lock().unwrap().is_empty());
    }

    struct AsyncOnlyStatistics {
        inner: TestStatistics,
        gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl StatisticsReader for AsyncOnlyStatistics {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.inner.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            self.inner.incarnation
        }
        fn read_statistics(
            &self,
            _: StatisticsReadRequest,
        ) -> Result<StatisticsEvidence, ConnectorError> {
            panic!("async statistics must never invoke the joining reader")
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
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                assert_eq!(request.table.owner(), &self.inner.descriptor.instance_id);
                assert_eq!(request.data_version, data_version(b"data-v1"));
                assert_eq!(request.metrics.metrics(), metric_request().metrics());
                let gate = self.gate.lock().unwrap().take();
                if let Some(gate) = gate {
                    gate.await.expect("observation released");
                }
                Ok(self.inner.evidence.clone())
            })
        }
    }
    impl ConnectorStatistics for AsyncOnlyStatistics {}

    #[tokio::test]
    async fn asynchronous_resolution_waits_without_cache_lock_and_reuses_the_same_revision() {
        let resolver = UnifiedStatisticsResolver::default();
        let epoch = ProviderBindingEpoch::from_bytes([9; 16]);
        let (release, gate) = tokio::sync::oneshot::channel();
        let reader = AsyncOnlyStatistics {
            inner: TestStatistics {
                descriptor: descriptor("ice"),
                incarnation: epoch,
                evidence: evidence(),
            },
            gate: Mutex::new(Some(gate)),
            calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let table = resolved_table("ice", epoch);
        let mut waiting =
            Box::pin(resolver.resolve_async(&table, &reader, metric_request(), request_context()));
        assert!(futures::poll!(&mut waiting).is_pending());
        assert!(resolver.artifacts.try_lock().is_ok());
        release.send(()).unwrap();
        let first = waiting.await.unwrap();
        let second = resolver
            .resolve_async(&table, &reader, metric_request(), request_context())
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            reader.calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the provider is observed again; only the exact immutable revision is cached"
        );
    }

    #[tokio::test]
    async fn asynchronous_resolution_preserves_source_fences_and_rejects_foreign_evidence() {
        let resolver = UnifiedStatisticsResolver::default();
        let epoch = ProviderBindingEpoch::from_bytes([9; 16]);
        let wrong_evidence = StatisticsEvidence::try_new(
            data_version(b"foreign-data"),
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"rev-foreign")).unwrap(),
            StatisticsRowCoverage::AllVisibleRows,
            BTreeMap::new(),
        )
        .unwrap();
        let reader = AsyncOnlyStatistics {
            inner: TestStatistics {
                descriptor: descriptor("ice"),
                incarnation: epoch,
                evidence: wrong_evidence,
            },
            gate: Mutex::new(None),
            calls: std::sync::atomic::AtomicUsize::new(0),
        };
        assert!(matches!(
            resolver
                .resolve_async(
                    &resolved_table("other", epoch),
                    &reader,
                    metric_request(),
                    request_context()
                )
                .await,
            Err(StatisticsResolutionFailure::OwnerMismatch)
        ));
        assert_eq!(reader.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(matches!(
            resolver
                .resolve_async(
                    &resolved_table("ice", epoch),
                    &reader,
                    metric_request(),
                    request_context()
                )
                .await,
            Err(StatisticsResolutionFailure::DataVersionMismatch)
        ));
        assert_eq!(reader.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(resolver.artifacts.lock().unwrap().is_empty());
    }
}
