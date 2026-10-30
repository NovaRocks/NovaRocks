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
use crate::connector::unified_statistics::{
    ResolvedStatisticsTable, StatisticsResolutionFailure, UnifiedStatisticsResolver,
};
use crate::query_execution::kernels::{DmlExecutionKernel, QueryPreparationKernel};
use arrow::datatypes::DataType;
use novarocks_spi::connector::{StatisticsMetric, StatisticsMetricRequest};
use novarocks_sql::compiler::{StatisticsFact, StatisticsNeed};
use novarocks_sql::planning::catalog::materialization_statistics_facts;
use novarocks_sql::planning::dml::{
    DmlStatisticsEvidence, DmlStatisticsFailure, DmlStatisticsSnapshot,
};

/// The actual statistics observer's refusal and its request label travel
/// together. Existing legacy callers render only at their outer String port.
#[derive(Clone, Debug)]
pub(crate) struct StatisticsSnapshotFailure {
    pub(crate) label: String,
    pub(crate) error: novarocks_spi::connector::ConnectorError,
}
impl std::fmt::Display for StatisticsSnapshotFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "freeze statistics for {}: {}", self.label, self.error)
    }
}
impl std::error::Error for StatisticsSnapshotFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

#[derive(Clone, Default)]
/// Query-scoped handles for the one unified statistics resolver.  This is not
/// a provider registry: absent pins intentionally produce missing statistics
/// rather than a second latest-resolution path.
pub struct QueryStatisticsContext {
    snapshot: DmlStatisticsSnapshot,
}

impl QueryStatisticsContext {
    #[allow(
        dead_code,
        reason = "Retained as the explicit empty statistics snapshot constructor for planning callers."
    )]
    pub(crate) fn none() -> Self {
        Self::default()
    }

    #[allow(
        dead_code,
        reason = "Retained as the unavailable-statistics compatibility constructor for planning callers."
    )]
    pub(crate) fn unavailable() -> Self {
        Self::none()
    }

    pub(crate) async fn from_statistics_resolver_with_bindings_async(
        resolver: &impl QueryStatisticsResolver,
        bindings: Arc<QueryTableBindingStore>,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Self, String> {
        Self::from_statistics_resolver_with_bindings_async_typed(
            resolver,
            bindings,
            connector_context,
        )
        .await
        .map_err(|error| error.to_string())
    }

    pub(crate) async fn from_statistics_resolver_with_bindings_async_typed(
        resolver: &impl QueryStatisticsResolver,
        bindings: Arc<QueryTableBindingStore>,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Self, StatisticsSnapshotFailure> {
        Ok(Self {
            snapshot: DmlStatisticsSnapshot::from_evidence(
                project_statistics_evidence_async(
                    resolver.unified_statistics(),
                    &bindings,
                    connector_context,
                )
                .await?,
            ),
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
    #[allow(
        dead_code,
        reason = "Retained for planning callers that must retain the resolver handle rather than a borrowed view."
    )]
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

/// Observe the same captured binding set and metric request as the original
/// DML snapshot, using only the pinned asynchronous statistics capability.
async fn project_statistics_evidence_async(
    resolver: &UnifiedStatisticsResolver,
    bindings: &QueryTableBindingStore,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<DmlStatisticsEvidence>, StatisticsSnapshotFailure> {
    let mut evidence = Vec::new();
    for (binding_id, binding) in bindings.captured_bindings() {
        let facts = materialization_statistics_facts(&binding.resolved);
        let metrics = match metric_request(facts.columns()) {
            Ok(metrics) => metrics,
            Err(error) => {
                evidence.push(fatal_statistics_evidence(
                    binding_id,
                    facts.label(),
                    DmlStatisticsFailure::CorruptEvidence(format!("build metric request: {error}")),
                ));
                continue;
            }
        };
        evidence.push(
            project_binding_statistics_for_metrics_async(
                resolver,
                binding_id,
                facts.label(),
                facts.columns(),
                &binding,
                metrics.metrics(),
                connector_context,
            )
            .await?,
        );
    }
    Ok(evidence)
}

pub(crate) async fn resolve_statistics_need_async(
    resolver: &UnifiedStatisticsResolver,
    bindings: &QueryTableBindingStore,
    need: &StatisticsNeed,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatisticsFact, String> {
    let binding_id = need.binding();
    let evidence = match bindings.binding(binding_id) {
        Ok(binding) => {
            let facts = materialization_statistics_facts(&binding.resolved);
            project_binding_statistics_for_metrics_async(
                resolver,
                binding_id,
                facts.label(),
                facts.columns(),
                &binding,
                need.metrics(),
                connector_context,
            )
            .await
            .map_err(|error| error.to_string())?
        }
        // An unknown token is a contradiction with the exact catalog fact
        // that introduced this SQL binding. It must not become a best-effort
        // current lookup or a conservative Missing observation.
        Err(_) => {
            let label = format!("SQL binding {binding_id:?}");
            fatal_statistics_evidence(binding_id, &label, DmlStatisticsFailure::BindingMissing)
        }
    };
    StatisticsFact::try_new(need, need.metrics().to_vec(), evidence)
        .map_err(|error| format!("build statistics completion fact: {error}"))
}

async fn project_binding_statistics_for_metrics_async(
    resolver: &UnifiedStatisticsResolver,
    binding_id: novarocks_sql::binding::SqlTableBindingId,
    label: &str,
    columns: &[novarocks_types::schema::ColumnDef],
    binding: &QueryTableBinding,
    requested_metrics: &[StatisticsMetric],
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<DmlStatisticsEvidence, StatisticsSnapshotFailure> {
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
    let metrics = match StatisticsMetricRequest::try_new(requested_metrics.to_vec()) {
        Ok(metrics) => metrics,
        Err(error) => {
            return Ok(fatal_statistics_evidence(
                binding_id,
                label,
                DmlStatisticsFailure::CorruptEvidence(format!("build metric request: {error}")),
            ));
        }
    };
    let evidence = match resolver
        .resolve_async(
            &ResolvedStatisticsTable {
                table: pin.table.clone(),
                data_version: pin.data_version.clone(),
                incarnation: control_binding.incarnation(),
            },
            statistics.as_ref(),
            metrics,
            connector_context.clone(),
        )
        .await
    {
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
            return Err(StatisticsSnapshotFailure {
                label: label.to_owned(),
                error,
            });
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
        columns: columns.to_vec(),
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

/// The metric set a visible-row collection may ask of these columns.
///
/// Every requester of a *collection* builds its request here, because a
/// collection fails closed as a whole: `finish_visible_row` rejects the
/// result when any requested metric produced nothing, so two requesters that
/// disagree about what is askable do not degrade one column, they discard the
/// table. Read paths such as `SHOW TABLE STATS` are free to ask about a metric
/// that cannot exist, since an unavailable answer is a fine answer to a
/// question about what is known.
pub(crate) fn visible_row_metric_request<'a>(
    columns: impl IntoIterator<Item = (&'a str, &'a DataType)>,
) -> Result<StatisticsMetricRequest, novarocks_spi::connector::ConnectorError> {
    let columns = columns.into_iter();
    let mut metrics = Vec::with_capacity(1 + columns.size_hint().0 * 5);
    metrics.push(StatisticsMetric::RowCount);
    for (column_name, data_type) in columns {
        let name = Arc::<str>::from(column_name);
        metrics.push(StatisticsMetric::NullCount {
            column: Arc::clone(&name),
        });
        // A collection fails closed when any requested metric produces
        // nothing, and the collector answers a type it cannot bound with
        // silence rather than an error. Asking a `STRING` column for a
        // minimum would therefore throw away the row count, null counts, and
        // NDV sketches of every column in the table. Ask only for the bounds
        // that can exist; a column with no bound vocabulary still contributes
        // everything else it can measure.
        if statistics_scalar_bounds_supported(data_type) {
            metrics.push(StatisticsMetric::Minimum {
                column: Arc::clone(&name),
            });
            metrics.push(StatisticsMetric::Maximum {
                column: Arc::clone(&name),
            });
        }
        metrics.push(StatisticsMetric::AverageSize {
            column: Arc::clone(&name),
        });
        metrics.push(StatisticsMetric::ThetaNdv { column: name });
    }
    StatisticsMetricRequest::try_new(metrics)
}

fn statistics_scalar_bounds_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    ) || novarocks_types::largeint::is_largeint_data_type(data_type)
}

fn metric_request(
    columns: &[novarocks_types::schema::ColumnDef],
) -> Result<StatisticsMetricRequest, novarocks_spi::connector::ConnectorError> {
    visible_row_metric_request(
        columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type)),
    )
}

#[cfg(test)]
mod unified_tests {
    mod held_port_tests;

    use std::num::NonZeroU64;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorControlBinding, ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind,
        ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorMetadata, ConnectorProviderBinding, ConnectorProviderId, ConnectorRequestContext,
        ConnectorScan, ConnectorScanHandle, ConnectorScanPlanning, ConnectorStatistics,
        ConnectorTableHandle, ConnectorTableMetadata, ConnectorTableRequest, ProviderBindingEpoch,
        StatisticsDataVersion, StatisticsEvidence, StatisticsMetric, StatisticsReadRequest,
        StatisticsReader,
    };
    use novarocks_types::schema::ColumnDef;

    use super::*;

    fn column(name: &str) -> ColumnDef {
        ColumnDef::from_value_type(
            name.into(),
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            None,
        )
        .expect("declared statistics fixture column")
    }

    fn local_binding(catalog: &str, namespace: &str, table: &str, seed: u64) -> QueryTableBinding {
        let mut allocator = novarocks_sql::binding::SqlTableBindingAllocator::try_new_for_test(
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

    struct ContextObservingProvider {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        reads: AtomicUsize,
        requested_metrics: Mutex<Vec<Vec<StatisticsMetric>>>,
        async_reads: AtomicUsize,
        refusal: Mutex<Option<ConnectorError>>,
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
            _creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            Err(Self::unsupported().into())
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
        ) -> Result<ConnectorProviderBinding, ConnectorError> {
            Err(Self::unsupported())
        }
    }

    impl StatisticsReader for ContextObservingProvider {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }

        fn read_statistics(
            &self,
            request: StatisticsReadRequest,
        ) -> Result<StatisticsEvidence, ConnectorError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.requested_metrics
                .lock()
                .expect("statistics fixture metrics lock")
                .push(request.metrics.metrics().to_vec());
            if let Some(error) = self.refusal.lock().unwrap().as_ref() {
                return Err(error.clone());
            }
            if request.context.is_cancelled() {
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
                self.async_reads.fetch_add(1, Ordering::SeqCst);
                self.reads.fetch_add(1, Ordering::SeqCst);
                self.requested_metrics
                    .lock()
                    .expect("statistics fixture metrics lock")
                    .push(request.metrics.metrics().to_vec());
                if let Some(error) = self.refusal.lock().unwrap().as_ref() {
                    return Err(error.clone());
                }
                if request.context.is_cancelled() {
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
                    "asynchronous statistics fixture has no evidence",
                ))
            })
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
        binding.statistics_pin = Some(crate::connector::backend::ResolvedTableStatisticsPin {
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
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
            reads: AtomicUsize::new(0),
            requested_metrics: Mutex::new(Vec::new()),
            async_reads: AtomicUsize::new(0),
            refusal: Mutex::new(None),
        })
    }

    fn request_context(deadline: Instant, cancelled: bool) -> ConnectorRequestContext {
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        if cancelled {
            stop.request_stop();
        }
        ConnectorRequestContext::try_new(
            deadline,
            stop.view(),
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

    #[tokio::test]
    async fn freeze_projects_every_captured_binding_once() {
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
        let connector_context = crate::connector::connector_request_context(
            None,
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
        )
        .expect("connector context");
        let evidence = project_statistics_evidence_async(
            &UnifiedStatisticsResolver::default(),
            &bindings,
            &connector_context,
        )
        .await
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

    #[tokio::test]
    async fn async_snapshot_keeps_captured_bindings_and_the_original_metric_request() {
        let provider = observing_provider();
        let bindings = QueryTableBindingStore::try_new().unwrap();
        bindings.insert_strict_base_binding_for_test(
            "ice.main",
            "db",
            "orders",
            connector_binding_with_statistics(provider.clone()),
        );
        bindings.insert_strict_base_binding_for_test(
            "ice.main",
            "db",
            "unavailable",
            local_binding("ice.main", "db", "unavailable", 74),
        );
        let captured = bindings.captured_bindings();
        let expected = metric_request(&[column("k")]).unwrap();
        let context = request_context(Instant::now() + Duration::from_secs(60), false);
        let evidence = project_statistics_evidence_async(
            &UnifiedStatisticsResolver::default(),
            &bindings,
            &context,
        )
        .await
        .unwrap();
        assert_eq!(evidence.len(), captured.len());
        for (binding_id, _) in captured {
            assert!(evidence.iter().any(|fact| matches!(fact,
                DmlStatisticsEvidence::Missing { binding, .. } if *binding == binding_id)));
        }
        assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider.async_reads.load(Ordering::SeqCst),
            1,
            "the async snapshot must never use the synchronous statistics reader"
        );
        assert_eq!(
            provider.requested_metrics.lock().unwrap().as_slice(),
            [expected.metrics().to_vec()]
        );
    }

    #[tokio::test]
    async fn async_snapshot_preserves_the_actual_statistics_refusal_and_shared_cause() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        let provider = observing_provider();
        let original = ConnectorError::from_domain(
            ConnectorErrorKind::Cancelled,
            ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "original statistics reader stop",
            ),
        );
        *provider.refusal.lock().unwrap() = Some(original.clone());
        let bindings = QueryTableBindingStore::try_new().unwrap();
        bindings.insert_strict_base_binding_for_test(
            "ice.main",
            "db",
            "orders",
            connector_binding_with_statistics(provider.clone()),
        );
        let context = request_context(Instant::now() + Duration::from_secs(60), false);
        let error = project_statistics_evidence_async(
            &UnifiedStatisticsResolver::default(),
            &bindings,
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(error.error, original);
        assert_eq!(error.error.failure_fact(), original.failure_fact());
        let cause = std::error::Error::source(&error.error)
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        let expected = std::error::Error::source(&original)
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        assert_eq!(cause, expected);
        assert_eq!(provider.async_reads.load(Ordering::SeqCst), 1);
        assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn freeze_statistics_provider_read_observes_caller_cancellation() {
        let provider = observing_provider();
        let bindings = QueryTableBindingStore::try_new().expect("binding store");
        bindings.insert_strict_base_binding_for_test(
            "ice.main",
            "db",
            "orders",
            connector_binding_with_statistics(provider.clone()),
        );
        let context = request_context(Instant::now() + Duration::from_secs(60), true);

        let error = project_statistics_evidence_async(
            &UnifiedStatisticsResolver::default(),
            &bindings,
            &context,
        )
        .await
        .expect_err("caller cancellation must stop statistics freeze");

        assert!(
            error
                .to_string()
                .contains("statistics fixture observed caller cancellation")
        );
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(error.error.kind(), ConnectorErrorKind::Cancelled);
        assert_eq!(
            error.error.failure_fact(),
            ConnectorErrorKind::Cancelled.failure_fact()
        );
        assert!(std::error::Error::source(&error.error).is_none());
        assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
        assert_eq!(provider.async_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn freeze_statistics_provider_read_observes_caller_deadline() {
        let provider = observing_provider();
        let bindings = QueryTableBindingStore::try_new().expect("binding store");
        bindings.insert_strict_base_binding_for_test(
            "ice.main",
            "db",
            "orders",
            connector_binding_with_statistics(provider.clone()),
        );
        let context = request_context(Instant::now() - Duration::from_secs(1), false);

        let error = project_statistics_evidence_async(
            &UnifiedStatisticsResolver::default(),
            &bindings,
            &context,
        )
        .await
        .expect_err("caller deadline must stop statistics freeze");

        assert!(
            error
                .to_string()
                .contains("statistics fixture observed caller deadline")
        );
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(error.error.kind(), ConnectorErrorKind::DeadlineExceeded);
        assert_eq!(
            error.error.failure_fact(),
            ConnectorErrorKind::DeadlineExceeded.failure_fact()
        );
        assert!(std::error::Error::source(&error.error).is_none());
        assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
        assert_eq!(provider.async_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn per_need_projection_preserves_the_exact_metric_request() {
        let provider = observing_provider();
        let binding = connector_binding_with_statistics(provider.clone());
        let binding_id = novarocks_sql::planning::catalog::table_binding_id(&binding.resolved);
        let requested = vec![
            StatisticsMetric::RowCount,
            StatisticsMetric::ThetaNdv {
                column: Arc::from("k"),
            },
        ];
        let context = request_context(Instant::now() + Duration::from_secs(60), false);

        let evidence = project_binding_statistics_for_metrics_async(
            &UnifiedStatisticsResolver::default(),
            binding_id,
            "ice.main.db.orders",
            &[column("k")],
            &binding,
            &requested,
            &context,
        )
        .await
        .expect("ordinary provider statistics failure remains Missing");

        assert!(matches!(evidence, DmlStatisticsEvidence::Missing { .. }));
        assert_eq!(
            provider
                .requested_metrics
                .lock()
                .expect("statistics fixture metrics lock")
                .as_slice(),
            [requested]
        );
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
