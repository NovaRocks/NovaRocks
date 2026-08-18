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

use crate::catalog_application::query_bindings::{QueryTableBinding, QueryTableBindingStore};
#[cfg(test)]
use crate::catalog_application::query_bindings::{
    QueryTableBindingAdmission, parse_time_travel_overlay_identity,
};
use crate::query_execution::kernels::{
    DmlExecutionKernel, MvExecutionKernel, QueryPreparationKernel,
};
use novarocks::connector::unified_statistics::{
    ResolvedStatisticsTable, StatisticsResolutionFailure, UnifiedStatisticsResolver,
};
use novarocks_spi::connector::{StatisticsMetric, StatisticsMetricRequest};
use novarocks_sql::planning::catalog::materialization_statistics_facts;
use novarocks_sql::planning::dml::{
    DmlStatisticsEvidence, DmlStatisticsFailure, DmlStatisticsSnapshot,
};

#[derive(Clone, Default)]
/// Query-scoped handles for the one unified statistics resolver.  This is not
/// a provider registry: absent pins intentionally produce missing statistics
/// rather than a second latest-resolution path.
pub struct QueryStatisticsContext {
    snapshot: DmlStatisticsSnapshot,
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
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Self, String> {
        Ok(Self {
            snapshot: DmlStatisticsSnapshot::from_evidence(project_statistics_evidence(
                resolver.unified_statistics(),
                &bindings,
                connector_context,
            )?),
        })
    }

    pub(crate) fn snapshot(&self) -> &DmlStatisticsSnapshot {
        &self.snapshot
    }
}

impl std::ops::Deref for QueryStatisticsContext {
    type Target = DmlStatisticsSnapshot;

    fn deref(&self) -> &Self::Target {
        self.snapshot()
    }
}

/// Query planning needs only frozen statistics evidence.  This trait avoids
/// taking the full application state while preserving the no-latest-lookup
/// rule in `QueryStatisticsContext`.
pub(crate) trait QueryStatisticsResolver {
    fn unified_statistics(&self) -> &UnifiedStatisticsResolver;
    fn unified_statistics_arc(&self) -> &Arc<UnifiedStatisticsResolver>;
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

/// Project every admission-frozen connector observation into SQL values before
/// optimization begins.  This is the one application boundary that may touch
/// a lease, a table handle, or a connector capability; `QueryStatisticsContext`
/// subsequently serves only the immutable snapshot below.
fn project_statistics_evidence(
    resolver: &UnifiedStatisticsResolver,
    bindings: &QueryTableBindingStore,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<DmlStatisticsEvidence>, String> {
    let mut evidence = Vec::new();
    for (binding_id, binding) in bindings.captured_bindings() {
        let facts = materialization_statistics_facts(&binding.resolved);
        evidence.push(project_binding_statistics(
            resolver,
            binding_id,
            &facts,
            &binding,
            connector_context,
        )?);
    }
    Ok(evidence)
}

fn project_binding_statistics(
    resolver: &UnifiedStatisticsResolver,
    binding_id: novarocks_sql::binding::SqlTableBindingId,
    facts: &novarocks_sql::planning::catalog::SqlCatalogStatisticsFacts,
    binding: &QueryTableBinding,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<DmlStatisticsEvidence, String> {
    let label = facts.label();
    let Some(pin) = binding.statistics_pin.as_ref() else {
        return Ok(DmlStatisticsEvidence::Missing {
            binding: binding_id,
            label: label.to_string(),
            reason: "resolved table does not expose connector statistics".to_string(),
        });
    };
    let planning_lease = match binding.admission.exact_planning_lease() {
        Ok(lease) => lease,
        Err(_) => {
            return Ok(fatal_statistics_evidence(
                binding_id,
                label,
                DmlStatisticsFailure::BindingMissing,
            ));
        }
    };
    let control_binding = planning_lease.binding();
    if control_binding.descriptor().instance_id != *pin.table.owner() {
        return Ok(fatal_statistics_evidence(
            binding_id,
            label,
            DmlStatisticsFailure::OwnerMismatch,
        ));
    }
    let Some(statistics) = control_binding.statistics() else {
        return Ok(DmlStatisticsEvidence::Missing {
            binding: binding_id,
            label: label.to_string(),
            reason: "resolved connector generation does not expose statistics".to_string(),
        });
    };
    let metrics = match metric_request(facts.columns()) {
        Ok(metrics) => metrics,
        Err(error) => {
            return Ok(fatal_statistics_evidence(
                binding_id,
                label,
                DmlStatisticsFailure::CorruptEvidence(format!("build metric request: {error}")),
            ));
        }
    };
    let evidence = match resolver.resolve(
        &ResolvedStatisticsTable {
            table: pin.table.clone(),
            data_version: pin.data_version.clone(),
            incarnation: control_binding.incarnation(),
        },
        statistics.as_ref(),
        metrics,
        connector_context.clone(),
    ) {
        Ok(evidence) => evidence,
        // A provider that cannot supply evidence remains the normal
        // conservative path.  Only a fact that contradicts the retained
        // binding is fatal to compilation.
        Err(StatisticsResolutionFailure::Connector(error))
            if matches!(
                error.kind(),
                novarocks_spi::connector::ConnectorErrorKind::Cancelled
                    | novarocks_spi::connector::ConnectorErrorKind::DeadlineExceeded
            ) =>
        {
            return Err(format!("freeze statistics for {label}: {error}"));
        }
        Err(StatisticsResolutionFailure::Connector(error)) => {
            return Ok(DmlStatisticsEvidence::Missing {
                binding: binding_id,
                label: label.to_string(),
                reason: error.to_string(),
            });
        }
        Err(error) => {
            return Ok(fatal_statistics_evidence(
                binding_id,
                label,
                map_resolution_failure(error),
            ));
        }
    };
    Ok(DmlStatisticsEvidence::Available {
        binding: binding_id,
        label: label.to_string(),
        columns: facts.columns().to_vec(),
        evidence: (*evidence).clone(),
    })
}

fn fatal_statistics_evidence(
    binding: novarocks_sql::binding::SqlTableBindingId,
    label: &str,
    failure: DmlStatisticsFailure,
) -> DmlStatisticsEvidence {
    DmlStatisticsEvidence::Fatal {
        binding,
        label: label.to_string(),
        failure,
    }
}

fn map_resolution_failure(error: StatisticsResolutionFailure) -> DmlStatisticsFailure {
    match error {
        StatisticsResolutionFailure::OwnerMismatch => DmlStatisticsFailure::OwnerMismatch,
        StatisticsResolutionFailure::IncarnationMismatch => {
            DmlStatisticsFailure::IncarnationMismatch
        }
        StatisticsResolutionFailure::DataVersionMismatch => {
            DmlStatisticsFailure::DataVersionMismatch
        }
        StatisticsResolutionFailure::CorruptEvidence(message) => {
            DmlStatisticsFailure::CorruptEvidence(message)
        }
        StatisticsResolutionFailure::Connector(error) => DmlStatisticsFailure::CorruptEvidence(
            format!("unexpected connector error after conservative mapping: {error}"),
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

#[cfg(test)]
mod unified_tests {
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;
    use bytes::Bytes;
    use novarocks_catalog::schema::ColumnDef;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorControlBinding, ConnectorControlPlanningLease,
        ConnectorError, ConnectorErrorKind, ConnectorExecutionDeclaration,
        ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorMetadata, ConnectorProviderId,
        ConnectorRequestContext, ConnectorScan, ConnectorScanHandle, ConnectorScanPlanning,
        ConnectorStatistics, ConnectorTableHandle, ConnectorTableMetadata, ConnectorTableRequest,
        StatisticsDataVersion, StatisticsEvidence, StatisticsMetric, StatisticsReadRequest,
        StatisticsReader,
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

    fn local_binding(catalog: &str, namespace: &str, table: &str, seed: u64) -> QueryTableBinding {
        let mut allocator = novarocks_sql::binding::SqlTableBindingAllocator::try_new(
            NonZeroU64::new(seed).expect("non-zero fixture scope"),
        )
        .expect("binding allocator");
        let binding = allocator.allocate().expect("binding token");
        let resolved = novarocks_sql::planning::catalog::materialize_connector_read_table(
            novarocks_sql::planning::catalog::ConnectorReadTableFacts {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                columns: vec![column("k")],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                schema: Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("k", DataType::Int64, true),
                ])),
                binding,
                selector: novarocks_spi::connector::ConnectorReadSelector::Current,
                planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
            },
        )
        .expect("local materialization")
        .into_resolved_table();
        QueryTableBinding::local(resolved, binding)
    }

    struct TestCancellation {
        cancelled: Arc<AtomicBool>,
    }

    impl ConnectorCancellation for TestCancellation {
        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::SeqCst)
        }
    }

    struct ContextObservingProvider {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        reads: AtomicUsize,
    }

    impl ContextObservingProvider {
        fn unsupported() -> ConnectorError {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "statistics context fixture only supports statistics reads",
            )
        }
    }

    impl ConnectorMetadata for ContextObservingProvider {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.descriptor.instance_id
        }

        fn namespace_exists(
            &self,
            _request: novarocks_spi::connector::ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            Err(Self::unsupported())
        }

        fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            Err(Self::unsupported())
        }

        fn list_tables(
            &self,
            _request: novarocks_spi::connector::ConnectorListTablesRequest,
        ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
            Err(Self::unsupported())
        }

        fn load_table(
            &self,
            _request: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            Err(Self::unsupported())
        }
    }

    impl ConnectorScanPlanning for ContextObservingProvider {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.descriptor.instance_id
        }

        fn begin_scan(
            &self,
            _table: &ConnectorTableHandle,
            _request: novarocks_spi::connector::ConnectorBeginScanRequest,
        ) -> Result<ConnectorScan, ConnectorError> {
            Err(Self::unsupported())
        }

        fn plan_splits(
            &self,
            _scan: &ConnectorScanHandle,
            _request: novarocks_spi::connector::ConnectorSplitPlanningRequest,
        ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError>
        {
            Err(Self::unsupported())
        }
    }

    impl ConnectorExecutionDistribution for ContextObservingProvider {
        fn declaration(
            &self,
            _context: &ConnectorRequestContext,
        ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
            Err(Self::unsupported())
        }
    }

    impl StatisticsReader for ContextObservingProvider {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ConnectorInstanceIncarnation {
            self.incarnation
        }

        fn read_statistics(
            &self,
            request: StatisticsReadRequest,
        ) -> Result<StatisticsEvidence, ConnectorError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if request.context.cancellation().is_cancelled() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Cancelled,
                    "statistics fixture observed caller cancellation",
                ));
            }
            if Instant::now() >= request.context.deadline() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::DeadlineExceeded,
                    "statistics fixture observed caller deadline",
                ));
            }
            Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "statistics fixture expected cancellation or deadline",
            ))
        }
    }

    impl ConnectorStatistics for ContextObservingProvider {}

    fn connector_binding_with_statistics(
        provider: Arc<ContextObservingProvider>,
    ) -> QueryTableBinding {
        let metadata: Arc<dyn ConnectorMetadata> = provider.clone();
        let planning: Arc<dyn ConnectorScanPlanning> = provider.clone();
        let distribution: Arc<dyn ConnectorExecutionDistribution> = provider.clone();
        let statistics: Arc<dyn ConnectorStatistics> = provider.clone();
        let control = Arc::new(
            ConnectorControlBinding::try_new_with_statistics(
                provider.descriptor.clone(),
                provider.incarnation,
                metadata,
                planning,
                distribution,
                None,
                Some(statistics),
            )
            .expect("statistics fixture control binding"),
        );
        let planning_lease = ConnectorControlPlanningLease::new(control, || {});
        let mut binding = local_binding("ice.main", "db", "orders", 73);
        binding.statistics_pin = Some(novarocks::connector::backend::ResolvedTableStatisticsPin {
            table: ConnectorTableHandle::try_new(
                provider.descriptor.instance_id.clone(),
                Bytes::from_static(b"orders"),
            )
            .expect("table handle"),
            data_version: StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1"))
                .expect("data version"),
        });
        binding.admission = QueryTableBindingAdmission::Exact(planning_lease);
        binding
    }

    fn observing_provider() -> Arc<ContextObservingProvider> {
        Arc::new(ContextObservingProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                instance_id: ConnectorInstanceId::parse("ice.main").expect("instance ID"),
            },
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
            reads: AtomicUsize::new(0),
        })
    }

    fn request_context(deadline: Instant, cancelled: Arc<AtomicBool>) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            deadline,
            Arc::new(TestCancellation { cancelled }),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request context")
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
    fn freeze_projects_every_captured_binding_once() {
        let bindings = QueryTableBindingStore::try_new().expect("binding store");
        bindings.insert_strict_base_binding_for_test(
            "iceberg",
            "db",
            "orders",
            local_binding("iceberg", "db", "orders", 71),
        );
        bindings.insert_strict_base_binding_for_test(
            "iceberg",
            "db",
            "mv_target",
            local_binding("iceberg", "db", "mv_target", 72),
        );
        let captured = bindings.captured_bindings();
        let connector_context = novarocks::connector::connector_request_context(
            None,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .expect("connector context");
        let evidence = project_statistics_evidence(
            &UnifiedStatisticsResolver::default(),
            &bindings,
            &connector_context,
        )
        .expect("statistics projection");

        assert_eq!(captured.len(), 2);
        assert_eq!(evidence.len(), 2);
        let captured_ids = captured
            .into_iter()
            .map(|(binding, _)| binding)
            .collect::<Vec<_>>();
        let evidence_ids = evidence
            .into_iter()
            .map(|entry| match entry {
                DmlStatisticsEvidence::Missing { binding, .. } => binding,
                other => panic!("local binding must project typed Missing, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert!(
            captured_ids
                .iter()
                .all(|binding| evidence_ids.contains(binding))
        );
    }

    #[test]
    fn freeze_statistics_provider_read_observes_caller_cancellation() {
        let provider = observing_provider();
        let bindings = QueryTableBindingStore::try_new().expect("binding store");
        bindings.insert_strict_base_binding_for_test(
            "ice.main",
            "db",
            "orders",
            connector_binding_with_statistics(provider.clone()),
        );
        let context = request_context(
            Instant::now() + Duration::from_secs(60),
            Arc::new(AtomicBool::new(true)),
        );

        let error =
            project_statistics_evidence(&UnifiedStatisticsResolver::default(), &bindings, &context)
                .expect_err("caller cancellation must stop statistics freeze");

        assert!(error.contains("statistics fixture observed caller cancellation"));
        assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn freeze_statistics_provider_read_observes_caller_deadline() {
        let provider = observing_provider();
        let bindings = QueryTableBindingStore::try_new().expect("binding store");
        bindings.insert_strict_base_binding_for_test(
            "ice.main",
            "db",
            "orders",
            connector_binding_with_statistics(provider.clone()),
        );
        let context = request_context(
            Instant::now() - Duration::from_secs(1),
            Arc::new(AtomicBool::new(false)),
        );

        let error =
            project_statistics_evidence(&UnifiedStatisticsResolver::default(), &bindings, &context)
                .expect_err("caller deadline must stop statistics freeze");

        assert!(error.contains("statistics fixture observed caller deadline"));
        assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
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
