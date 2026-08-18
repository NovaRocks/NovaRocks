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

//! Provider-neutral statistics collection values.
//!
//! This module deliberately has no `QueryResult` dependency. Statistics
//! collection is internal distributed work whose output is handed back to the
//! connector control plane, never encoded as client MySQL rows.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datasketches::theta::ThetaSketch;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorControlPlanningLease,
    ConnectorControlResolver, ConnectorReadSelector, ConnectorRequestContext,
    ConnectorSplitPlanningRequest, StatisticsBasisRelation, StatisticsCollectionPlan,
    StatisticsCollectionResult, StatisticsDataVersion, StatisticsEvidence,
    StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricObservation,
    StatisticsMetricRequest, StatisticsMetricSource, StatisticsMetricState, StatisticsMetricValue,
    StatisticsMissing, StatisticsMissingKind, StatisticsNumericNature, StatisticsRowCoverage,
};

use crate::common::backend_topology::BackendTopologySnapshot;
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryRequest,
    build_statistics_query_request_with_execution,
};
use crate::query_execution::preparation::scan::{
    PlannedConnectorRead, ResolvedScanExecution, ScanBindingResolver,
};
use sha2::{Digest, Sha256};

/// The longest independently owned durable ANALYZE attempt. A client wait
/// deadline is intentionally not an attempt deadline.
pub const MAX_STATISTICS_ATTEMPT_DURATION: Duration = Duration::from_secs(30 * 60);

/// Bound the in-memory, mergeable Theta state produced by one statistics
/// collection. This is independent of the SPI's wire-payload bound.
/// The SPI's complete opaque result is capped at 64 KiB.  Keep one Theta
/// state safely below that ceiling so it can coexist with a data-version and
/// other requested metrics; a larger sketch would only fail late at the SPI
/// boundary after distributed work had already completed.
pub const MAX_STATISTICS_THETA_RETAINED_HASHES: usize = 1 << 12;
const THETA_PARTIAL_WIRE_VERSION: u8 = 1;
const THETA_PARTIAL_WIRE_HEADER_BYTES: usize = 1 + 1 + 8 + 4;
const VISIBLE_ROW_ARTIFACT_VERSION: u8 = 1;
/// Versioned BE-to-coordinator payload carrying one mergeable visible-row
/// collection partial.  It is deliberately separate from the provider
/// artifact: this payload crosses only the native execution lifecycle,
/// whereas the provider artifact is retained only after finalization.
const STATISTICS_FRAGMENT_PAYLOAD_VERSION: u8 = 2;

/// A statistics collection is either tied to a statement wait or owned by a
/// durable frontend job. Only the former observes the statement cancellation
/// view supplied to distributed execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsExecutionMode {
    SynchronousWait,
    DurableJobAttempt,
}

impl StatisticsExecutionMode {
    pub const fn statement_cancellation_terminates_execution(self) -> bool {
        matches!(self, Self::SynchronousWait)
    }

    pub const fn maximum_attempt_duration(self) -> Duration {
        MAX_STATISTICS_ATTEMPT_DURATION
    }
}

/// Validated execution policy handed from the application service to Core.
/// A durable job's explicit cancellation is delivered by its worker control
/// plane; it must not be synthesized from a disconnected SQL client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatisticsExecutionPolicy {
    mode: StatisticsExecutionMode,
    attempt_timeout: Duration,
}

impl StatisticsExecutionPolicy {
    pub fn try_new(
        mode: StatisticsExecutionMode,
        attempt_timeout: Duration,
    ) -> Result<Self, DistributedQueryError> {
        if attempt_timeout.is_zero() || attempt_timeout > MAX_STATISTICS_ATTEMPT_DURATION {
            return Err(contract_violation(format!(
                "statistics attempt timeout must be between 1ns and {} seconds",
                MAX_STATISTICS_ATTEMPT_DURATION.as_secs()
            )));
        }
        Ok(Self {
            mode,
            attempt_timeout,
        })
    }

    pub const fn mode(self) -> StatisticsExecutionMode {
        self.mode
    }

    pub const fn attempt_timeout(self) -> Duration {
        self.attempt_timeout
    }
}

/// A Core-owned compilation input. Connector-specific instructions remain in
/// the bounded opaque SPI plan; Core only owns its lifecycle and result sink.
#[derive(Clone)]
pub struct StatisticsCollectionProgram {
    plan: StatisticsCollectionPlan,
    policy: StatisticsExecutionPolicy,
}

impl StatisticsCollectionProgram {
    pub fn try_new(
        plan: StatisticsCollectionPlan,
        policy: StatisticsExecutionPolicy,
    ) -> Result<Self, DistributedQueryError> {
        if plan.data_version.as_bytes().is_empty() {
            return Err(contract_violation(
                "statistics collection plan has an empty data-version token",
            ));
        }
        if plan.evidence_revision().as_bytes().is_empty() {
            return Err(contract_violation(
                "statistics collection plan has an empty evidence-revision token",
            ));
        }
        let distinct_metrics = plan.metrics.metrics().iter().collect::<BTreeSet<_>>();
        if distinct_metrics.len() != plan.metrics.metrics().len() {
            return Err(contract_violation(
                "statistics collection plan contains duplicate metrics",
            ));
        }
        Ok(Self { plan, policy })
    }

    pub fn plan(&self) -> &StatisticsCollectionPlan {
        &self.plan
    }

    pub const fn policy(&self) -> StatisticsExecutionPolicy {
        self.policy
    }

    pub fn result_sink(&self) -> StatisticsResultSink {
        StatisticsResultSink {
            data_version: self.plan.data_version.clone(),
            metrics: self.plan.metrics.clone(),
            result: None,
        }
    }

    /// Finalize the non-empty native fragment reports received by the
    /// coordinator. Empty reports are permitted for non-root fragments in an
    /// exchange-shaped collection, but a successful collection without any
    /// bounded partial is always rejected rather than treated as zero rows.
    pub fn finish_fragment_payloads<T>(
        &self,
        payloads: impl IntoIterator<Item = T>,
    ) -> Result<StatisticsCollectionResult, DistributedQueryError>
    where
        T: AsRef<[u8]>,
    {
        let partials = payloads
            .into_iter()
            .filter_map(|payload| {
                (!payload.as_ref().is_empty()).then(|| {
                    StatisticsCollectionFinalizer::try_from_fragment_payload(payload.as_ref())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if partials.is_empty() {
            return Err(contract_violation(
                "statistics collection completed without a fragment partial",
            ));
        }
        StatisticsCollectionFinalizer::try_merge(partials)?.finish_visible_row(
            self.plan.data_version.clone(),
            self.plan.evidence_revision().clone(),
            &self.plan.metrics,
        )
    }
}

/// Prepare the provider-neutral read portion of a statistics collection using
/// the normal connector control path.  The table handle and physical
/// projection originate from the provider's one-time pinned metadata
/// resolution; this helper intentionally performs no catalog lookup and never
/// opens a BE reader locally.  The returned planning lease keeps that exact
/// connector generation live until the normal execution-binding barrier has
/// consumed the declaration.
pub(crate) fn prepare_statistics_connector_read(
    lease: ConnectorControlPlanningLease,
    topology: &BackendTopologySnapshot,
    program: &StatisticsCollectionProgram,
    context: ConnectorRequestContext,
) -> Result<PlannedConnectorRead, DistributedQueryError> {
    let target_parallelism = statistics_target_parallelism(topology)?;
    if lease.binding().descriptor().instance_id != *program.plan.table().owner() {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "statistics collection planning lease does not own its resolved table handle",
        ));
    }
    let batch = ConnectorBatchBudget {
        max_rows: NonZeroUsize::new(4096).expect("statistics batch rows are nonzero"),
        max_bytes: NonZeroUsize::new(context.max_handle_payload_bytes())
            .expect("validated connector payload budget is nonzero"),
    };
    // `Current` here is not a latest metadata lookup: the opaque table handle
    // is the provider-resolved data-version pin.  Providers must bind this
    // selector to that handle's version, as Iceberg does with snapshot_id.
    let scan = lease
        .binding()
        .planning()
        .begin_scan(
            program.plan.table(),
            ConnectorBeginScanRequest {
                projection: program.plan.scan_projection(),
                static_predicates: Vec::new(),
                selection: novarocks_spi::connector::ConnectorScanSelection::Snapshot(
                    ConnectorReadSelector::Current,
                ),
                purpose: novarocks_spi::connector::ConnectorReadPurpose::Query,
                limit: None,
                batch,
                context: context.clone(),
            },
        )
        .map_err(connector_planning_error)?;
    scan.validate(
        &novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id: lease.binding().descriptor().instance_id.clone(),
            incarnation: lease.binding().incarnation(),
        },
        novarocks_spi::connector::ConnectorScanSelection::Snapshot(ConnectorReadSelector::Current),
    )
    .map_err(connector_planning_error)?;
    let split_result = lease
        .binding()
        .planning()
        .plan_splits(
            scan.handle(),
            ConnectorSplitPlanningRequest {
                target_parallelism,
                max_split_bytes: None,
                context: context.clone(),
            },
        )
        .map_err(connector_planning_error)?;
    if split_result
        .splits
        .iter()
        .any(|split| split.owner() != &lease.binding().descriptor().instance_id)
    {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "statistics collection provider planned a split for another connector instance",
        ));
    }
    let declaration = lease
        .binding()
        .execution_declaration(&context)
        .map_err(connector_planning_error)?;
    Ok(PlannedConnectorRead {
        declaration,
        provider_field_ordinals: (0..scan.output_schema().fields().len())
            .map(|ordinal| u32::try_from(ordinal).expect("connector output ordinal fits u32"))
            .collect(),
        scan,
        splits: split_result.splits,
        planning_metrics: split_result.metrics,
        static_predicates: Vec::new(),
        predicate_dispositions: Vec::new(),
        residual_predicates: Vec::new(),
        batch,
        planning_lease: lease,
        read_session: split_result.session,
    })
}

fn statistics_target_parallelism(
    topology: &BackendTopologySnapshot,
) -> Result<NonZeroUsize, DistributedQueryError> {
    NonZeroUsize::new(topology.targets().len()).ok_or_else(|| {
        DistributedQueryError::new(
            DistributedQueryErrorKind::Rejected,
            "statistics collection requires at least one live backend",
        )
    })
}

/// Prepared, one-shot statistics request assembly.
///
/// This is the statistics counterpart to the normal query assembly: provider
/// planning and fragment preparation happen once, then the caller receives a
/// borrow-only native encoding view.  `finish` accepts only the attachment
/// sealed by that exact view, so it cannot substitute a later provider read,
/// topology snapshot, cancellation view, or statistics program.
pub struct PreparedStatisticsCollectionRequest {
    encoding: crate::query_execution::post_compile::NativeFragmentEncodingInput,
    program: StatisticsCollectionProgram,
    execution: crate::common::admitted_query_context::QueryExecutionContext,
}

impl PreparedStatisticsCollectionRequest {
    /// Immutable facts consumed by the native encoder.  This has no acquire
    /// path: connector leases and read sessions remain sealed in preparation.
    pub fn encoding_view(
        &self,
    ) -> crate::query_execution::native_fragment::NativeFragmentEncodingView<'_> {
        self.encoding.encoding_view()
    }

    /// Consume the matching native attachment into the sole statistics
    /// distributed request.  Keeping the typed program here prevents a
    /// statistics attempt from degrading into a generic result request.
    pub fn finish(
        self,
        native_attachment: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<DistributedQueryRequest, DistributedQueryError> {
        if !self.encoding.matches_native_attachment(&native_attachment) {
            return Err(contract_violation(
                "native fragment bundle does not match the sealed statistics encoding input",
            ));
        }
        let (_, prepared) = self.encoding.into_parts();
        Ok(build_statistics_query_request_with_execution(
            prepared,
            native_attachment,
            None,
            self.program,
            &self.execution,
        ))
    }
}

/// Prepare an already-pinned connector statistics program for native
/// distributed execution. Preparation receives the opaque read through a
/// one-shot resolver; it cannot reopen the provider catalog or reinterpret
/// the table's data version. The emitted fragment source remains the regular
/// `ConnectorReadSource`, so BE execution has no statistics-only connector
/// identity or reader path.
pub fn prepare_statistics_collection_request(
    controls: &dyn ConnectorControlResolver,
    execution: &crate::common::admitted_query_context::QueryExecutionContext,
    context: ConnectorRequestContext,
    program: StatisticsCollectionProgram,
    planning_lease: ConnectorControlPlanningLease,
) -> Result<PreparedStatisticsCollectionRequest, DistributedQueryError> {
    let read = prepare_statistics_connector_read(
        planning_lease,
        execution.topology(),
        &program,
        context.clone(),
    )?;
    let table_bindings = Arc::new(
        novarocks::catalog_application::query_bindings::QueryTableBindingStore::try_new()
            .map_err(contract_violation)?,
    );
    let source_binding = admit_statistics_scan_binding(table_bindings.as_ref(), &program)?;
    let distributed = novarocks_sql::planning::dml::build_statistics_connector_plan(
        novarocks_sql::planning::dml::StatisticsConnectorScan {
            binding: source_binding,
            columns: program
                .plan
                .scan_columns()
                .iter()
                .map(|column| novarocks_catalog::schema::ColumnDef {
                    name: column.name().to_string(),
                    data_type: column.data_type().clone(),
                    nullable: column.nullable(),
                    write_default: None,
                    logical_type: None,
                })
                .collect(),
        },
        program.plan.metrics.clone(),
        execution.optimizer_settings(),
    )
    .map_err(contract_violation)?;
    let resolver = PinnedStatisticsReadResolver::new(read);
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        controls,
        &context,
        Some(table_bindings.as_ref()),
        Some(&resolver),
        crate::query_execution::preparation::ScanPreparationOptions::new(
            true,
            std::num::NonZeroUsize::new(execution.topology().targets().len()).ok_or_else(|| {
                contract_violation(
                    "statistics connector preparation requires a non-empty admitted backend topology",
                )
            })?,
            None,
        ),
    )
    .map_err(contract_violation)?;
    Ok(PreparedStatisticsCollectionRequest {
        encoding: crate::query_execution::post_compile::NativeFragmentEncodingInput::new(
            distributed,
            prepared,
        ),
        program,
        execution: execution.clone(),
    })
}

/// Retain a request-local SQL token even though the opaque statistics read is
/// supplied by `PinnedStatisticsReadResolver`.  The resolver holds the exact
/// statistics lease; the binding store keeps the synthetic compiler source
/// scoped to this one submission and prevents a fallback catalog acquire.
fn admit_statistics_scan_binding(
    bindings: &novarocks::catalog_application::query_bindings::QueryTableBindingStore,
    program: &StatisticsCollectionProgram,
) -> Result<novarocks_sql::binding::SqlTableBindingId, DistributedQueryError> {
    let input_schema = Arc::new(arrow::datatypes::Schema::new(
        program
            .plan
            .scan_columns()
            .iter()
            .map(|column| {
                arrow::datatypes::Field::new(
                    column.name(),
                    column.data_type().clone(),
                    column.nullable(),
                )
            })
            .collect::<Vec<_>>(),
    ));
    let identity = novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::new(
        "__statistics",
        "__statistics",
        "__connector_pinned_statistics",
    );
    crate::query_execution::frozen_connector_read::admit_frozen_connector_scan_binding(
        bindings,
        &identity,
        &input_schema,
    )
    .map_err(contract_violation)
}

struct PinnedStatisticsReadResolver {
    read: Mutex<Option<PlannedConnectorRead>>,
}

impl PinnedStatisticsReadResolver {
    fn new(read: PlannedConnectorRead) -> Self {
        Self {
            read: Mutex::new(Some(read)),
        }
    }
}

impl ScanBindingResolver for PinnedStatisticsReadResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        _scan: &novarocks_sql::plan_read::PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        Ok(Some(ResolvedScanExecution::ConnectorRead))
    }

    fn resolve_connector_read(
        &self,
        _node_id: i32,
        _scan: &novarocks_sql::plan_read::PlanScanNode,
    ) -> Result<Option<PlannedConnectorRead>, String> {
        self.read
            .lock()
            .map_err(|_| "pinned statistics connector read lock poisoned".to_string())
            .map(|mut read| read.take())
    }
}

fn connector_planning_error(
    error: novarocks_spi::connector::ConnectorError,
) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, error.to_string())
}

/// A one-result bounded sink for an internal statistics execution. It rejects
/// version drift and metric-set expansion before a result can reach a
/// connector publisher.
pub struct StatisticsResultSink {
    data_version: novarocks_spi::connector::StatisticsDataVersion,
    metrics: StatisticsMetricRequest,
    result: Option<StatisticsCollectionResult>,
}

impl StatisticsResultSink {
    pub fn accept(
        &mut self,
        result: StatisticsCollectionResult,
    ) -> Result<(), DistributedQueryError> {
        if self.result.is_some() {
            return Err(contract_violation(
                "statistics result sink received more than one collection result",
            ));
        }
        let evidence = &result.evidence;
        if *evidence.data_version() != self.data_version {
            return Err(contract_violation(
                "statistics collection result does not match its pinned data version",
            ));
        }
        let expected = self.metrics.metrics().iter().collect::<BTreeSet<_>>();
        let actual = evidence.metrics().keys().collect::<BTreeSet<_>>();
        if actual != expected {
            return Err(contract_violation(
                "statistics collection result does not match its requested metric set",
            ));
        }
        self.result = Some(result);
        Ok(())
    }

    pub fn finish(self) -> Result<StatisticsCollectionResult, DistributedQueryError> {
        self.result.ok_or_else(|| {
            contract_violation("statistics result sink completed without a collection result")
        })
    }
}

/// A mergeable, Core-internal Theta partial. It never exposes a user SQL
/// aggregate and is used only by the statistics collector.
#[derive(Clone, Debug, PartialEq)]
pub struct ThetaSketchPartial {
    lg_k: u8,
    theta: u64,
    retained_hashes: Vec<u64>,
}

/// Finalized Theta output suitable for a typed statistics metric value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ThetaSketchFinal {
    estimate: f64,
}

/// Mergeable scalar partial for row/null/min/max/size collection. Bounds keep
/// their physical scalar type until the provider-owned artifact boundary;
/// they must never be coerced through strings or lossy floating-point values.
#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsScalarPartial {
    row_count: u64,
    null_count: u64,
    total_size: u64,
    minimum: Option<StatisticsScalarBound>,
    maximum: Option<StatisticsScalarBound>,
}

#[derive(Clone, Debug, PartialEq)]
enum StatisticsScalarBound {
    F64(f64),
    LargeInt(i128),
}

/// Per-fragment Arrow collector used by the native statistics sink.  It is
/// schema-bound at construction time, so logical metric names can never be
/// rebound to a different scan projection after the connector's table pin
/// has been resolved.
pub struct StatisticsBatchCollector {
    schema: SchemaRef,
    metrics: StatisticsMetricRequest,
    column_indexes: BTreeMap<std::sync::Arc<str>, usize>,
    table_rows: u64,
    columns: BTreeMap<std::sync::Arc<str>, StatisticsScalarAccumulator>,
    theta: BTreeMap<std::sync::Arc<str>, StatisticsThetaAccumulator>,
}

#[derive(Clone, Debug, Default)]
struct StatisticsScalarAccumulator {
    row_count: u64,
    null_count: u64,
    total_size: u64,
    minimum: Option<StatisticsScalarBound>,
    maximum: Option<StatisticsScalarBound>,
}

/// Keeps one bounded Theta sketch per requested column.  Batch input may be
/// arbitrarily large over the lifetime of a fragment, so retaining every
/// hashed value until `finish` would turn an approximate metric into an
/// unbounded memory sink.
#[derive(Debug)]
struct StatisticsThetaAccumulator {
    sketch: ThetaSketch,
}

impl StatisticsScalarPartial {
    pub fn try_new(
        row_count: u64,
        null_count: u64,
        total_size: u64,
        minimum: Option<f64>,
        maximum: Option<f64>,
    ) -> Result<Self, DistributedQueryError> {
        Self::try_new_bounds(
            row_count,
            null_count,
            total_size,
            minimum.map(StatisticsScalarBound::F64),
            maximum.map(StatisticsScalarBound::F64),
        )
    }

    fn try_new_bounds(
        row_count: u64,
        null_count: u64,
        total_size: u64,
        minimum: Option<StatisticsScalarBound>,
        maximum: Option<StatisticsScalarBound>,
    ) -> Result<Self, DistributedQueryError> {
        if null_count > row_count {
            return Err(contract_violation(
                "statistics null count exceeds row count",
            ));
        }
        if minimum.as_ref().is_some_and(|value| !value.is_valid())
            || maximum.as_ref().is_some_and(|value| !value.is_valid())
            || matches!((&minimum, &maximum), (Some(minimum), Some(maximum)) if minimum.compare(maximum).is_none_or(|order| order.is_gt()))
        {
            return Err(contract_violation(
                "statistics scalar bounds must be finite, equally typed, and ordered",
            ));
        }
        Ok(Self {
            row_count,
            null_count,
            total_size,
            minimum,
            maximum,
        })
    }

    pub fn try_merge(
        partials: impl IntoIterator<Item = Self>,
    ) -> Result<Self, DistributedQueryError> {
        let mut merged = Self::try_new_bounds(0, 0, 0, None, None)?;
        for partial in partials {
            merged.row_count = merged
                .row_count
                .checked_add(partial.row_count)
                .ok_or_else(|| resource_exhausted("statistics row count overflow"))?;
            merged.null_count = merged
                .null_count
                .checked_add(partial.null_count)
                .ok_or_else(|| resource_exhausted("statistics null count overflow"))?;
            merged.total_size = merged
                .total_size
                .checked_add(partial.total_size)
                .ok_or_else(|| resource_exhausted("statistics total size overflow"))?;
            merged.minimum = merge_scalar_bounds(merged.minimum, partial.minimum, true)?;
            merged.maximum = merge_scalar_bounds(merged.maximum, partial.maximum, false)?;
        }
        Self::try_new_bounds(
            merged.row_count,
            merged.null_count,
            merged.total_size,
            merged.minimum,
            merged.maximum,
        )
    }

    pub fn metric_values(
        &self,
        metrics: impl IntoIterator<Item = StatisticsMetric>,
    ) -> Result<BTreeMap<StatisticsMetric, StatisticsMetricValue>, DistributedQueryError> {
        let mut output = BTreeMap::new();
        for metric in metrics {
            let value = match &metric {
                StatisticsMetric::RowCount => StatisticsMetricValue::U64(self.row_count),
                StatisticsMetric::NullCount { .. } => StatisticsMetricValue::U64(self.null_count),
                StatisticsMetric::AverageSize { .. } => StatisticsMetricValue::F64(
                    (self.row_count != 0)
                        .then(|| self.total_size as f64 / self.row_count as f64)
                        .unwrap_or(0.0),
                ),
                StatisticsMetric::Minimum { .. } => self
                    .minimum
                    .as_ref()
                    .ok_or_else(|| contract_violation("statistics minimum is unavailable"))?
                    .metric_value(),
                StatisticsMetric::Maximum { .. } => self
                    .maximum
                    .as_ref()
                    .ok_or_else(|| contract_violation("statistics maximum is unavailable"))?
                    .metric_value(),
                StatisticsMetric::ThetaNdv { .. } => continue,
            };
            output.insert(metric, value);
        }
        Ok(output)
    }
}

impl StatisticsBatchCollector {
    pub fn try_new(
        schema: SchemaRef,
        metrics: StatisticsMetricRequest,
    ) -> Result<Self, DistributedQueryError> {
        let mut column_indexes = BTreeMap::new();
        for metric in metrics.metrics() {
            let Some(column) = statistics_metric_column(metric) else {
                continue;
            };
            let index = schema
                .fields()
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(column))
                .ok_or_else(|| {
                    contract_violation(format!(
                        "statistics scan schema does not contain requested column `{column}`"
                    ))
                })?;
            column_indexes.insert(column.clone(), index);
        }
        let scalar_columns = metrics
            .metrics()
            .iter()
            .filter_map(|metric| match metric {
                StatisticsMetric::NullCount { column }
                | StatisticsMetric::Minimum { column }
                | StatisticsMetric::Maximum { column }
                | StatisticsMetric::AverageSize { column } => Some(column.clone()),
                StatisticsMetric::RowCount | StatisticsMetric::ThetaNdv { .. } => None,
            })
            .collect::<BTreeSet<_>>();
        let theta_columns = metrics
            .metrics()
            .iter()
            .filter_map(|metric| match metric {
                StatisticsMetric::ThetaNdv { column } => Some(column.clone()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        Ok(Self {
            schema,
            metrics,
            column_indexes,
            table_rows: 0,
            columns: scalar_columns
                .into_iter()
                .map(|column| (column, StatisticsScalarAccumulator::default()))
                .collect(),
            theta: theta_columns
                .into_iter()
                .map(|column| (column, StatisticsThetaAccumulator::new(12)))
                .collect(),
        })
    }

    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<(), DistributedQueryError> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(contract_violation(
                "statistics batch schema differs from the pinned scan schema",
            ));
        }
        let rows = u64::try_from(batch.num_rows())
            .map_err(|_| resource_exhausted("statistics batch row count exceeds u64"))?;
        self.table_rows = self
            .table_rows
            .checked_add(rows)
            .ok_or_else(|| resource_exhausted("statistics row count overflow"))?;
        for (column, index) in &self.column_indexes {
            let array = batch.column(*index);
            if let Some(accumulator) = self.columns.get_mut(column) {
                accumulator.push(array, rows)?;
            }
            if let Some(accumulator) = self.theta.get_mut(column) {
                accumulator.push(array)?;
            }
        }
        Ok(())
    }

    pub fn finish(self) -> Result<StatisticsCollectionFinalizer, DistributedQueryError> {
        let table = StatisticsScalarPartial::try_new(self.table_rows, 0, 0, None, None)?;
        let mut finalizer = StatisticsCollectionFinalizer::default().with_table(table);
        for (column, accumulator) in self.columns {
            finalizer = finalizer.with_column(column, accumulator.finish()?);
        }
        for (column, accumulator) in self.theta {
            finalizer = finalizer.with_theta(column, accumulator.finish()?);
        }
        // Keep the requested metric set in the collector state so accidental
        // construction with an empty or replaced selection remains visible to
        // the compiler and does not silently widen collection behavior.
        debug_assert!(!self.metrics.metrics().is_empty());
        Ok(finalizer)
    }

    /// Finish one fragment's collection into the bounded terminal-report
    /// payload. The coordinator is the only component that may merge these
    /// payloads into provider-facing evidence.
    pub fn finish_fragment_payload(self) -> Result<Bytes, DistributedQueryError> {
        self.finish()?.try_to_fragment_payload()
    }
}

impl StatisticsScalarAccumulator {
    fn push(&mut self, array: &ArrayRef, rows: u64) -> Result<(), DistributedQueryError> {
        self.row_count = self
            .row_count
            .checked_add(rows)
            .ok_or_else(|| resource_exhausted("statistics row count overflow"))?;
        self.null_count = self
            .null_count
            .checked_add(
                u64::try_from(array.null_count())
                    .map_err(|_| resource_exhausted("statistics null count exceeds u64"))?,
            )
            .ok_or_else(|| resource_exhausted("statistics null count overflow"))?;
        self.total_size = self
            .total_size
            .checked_add(estimated_value_bytes(array)?)
            .ok_or_else(|| resource_exhausted("statistics total size overflow"))?;
        for value in array_scalar_bounds(array)? {
            self.minimum = merge_scalar_bounds(self.minimum.take(), Some(value.clone()), true)?;
            self.maximum = merge_scalar_bounds(self.maximum.take(), Some(value), false)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<StatisticsScalarPartial, DistributedQueryError> {
        StatisticsScalarPartial::try_new_bounds(
            self.row_count,
            self.null_count,
            self.total_size,
            self.minimum,
            self.maximum,
        )
    }
}

impl StatisticsThetaAccumulator {
    fn new(lg_k: u8) -> Self {
        debug_assert!((5..=12).contains(&lg_k));
        Self {
            sketch: ThetaSketch::builder().lg_k(lg_k).build(),
        }
    }

    fn push(&mut self, array: &ArrayRef) -> Result<(), DistributedQueryError> {
        for hash in array_hashes(array)? {
            // `ThetaSketch` performs bounded sampling internally. The SHA-256
            // value keeps the Arrow representation out of the sketch's public
            // hash domain and makes every supported scalar type deterministic.
            self.sketch.update(hash as i64);
        }
        Ok(())
    }

    fn finish(self) -> Result<ThetaSketchPartial, DistributedQueryError> {
        ThetaSketchPartial::try_from_sketch(self.sketch)
    }
}

fn statistics_metric_column(metric: &StatisticsMetric) -> Option<&std::sync::Arc<str>> {
    match metric {
        StatisticsMetric::RowCount => None,
        StatisticsMetric::NullCount { column }
        | StatisticsMetric::Minimum { column }
        | StatisticsMetric::Maximum { column }
        | StatisticsMetric::AverageSize { column }
        | StatisticsMetric::ThetaNdv { column } => Some(column),
    }
}

fn estimated_value_bytes(array: &ArrayRef) -> Result<u64, DistributedQueryError> {
    let bytes = if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        array
            .iter()
            .flatten()
            .map(|value| value.len() as u64)
            .try_fold(0_u64, |total, value| total.checked_add(value).ok_or(()))
            .map_err(|_| resource_exhausted("statistics string size overflow"))?
    } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        array
            .iter()
            .flatten()
            .map(|value| value.len() as u64)
            .try_fold(0_u64, |total, value| total.checked_add(value).ok_or(()))
            .map_err(|_| resource_exhausted("statistics string size overflow"))?
    } else {
        u64::try_from(array.get_array_memory_size())
            .map_err(|_| resource_exhausted("statistics value size exceeds u64"))?
    };
    Ok(bytes)
}

impl StatisticsScalarBound {
    fn is_valid(&self) -> bool {
        !matches!(self, Self::F64(value) if !value.is_finite())
    }

    fn compare(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::F64(left), Self::F64(right)) => left.partial_cmp(right),
            (Self::LargeInt(left), Self::LargeInt(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }

    fn metric_value(&self) -> StatisticsMetricValue {
        match self {
            Self::F64(value) => StatisticsMetricValue::F64(*value),
            Self::LargeInt(value) => {
                StatisticsMetricValue::Bytes(Bytes::copy_from_slice(&value.to_be_bytes()))
            }
        }
    }
}

fn merge_scalar_bounds(
    left: Option<StatisticsScalarBound>,
    right: Option<StatisticsScalarBound>,
    minimum: bool,
) -> Result<Option<StatisticsScalarBound>, DistributedQueryError> {
    match (left, right) {
        (Some(left), Some(right)) => {
            let ordering = left.compare(&right).ok_or_else(|| {
                contract_violation("statistics scalar bounds use incompatible physical types")
            })?;
            Ok(Some(
                if (minimum && ordering.is_gt()) || (!minimum && ordering.is_lt()) {
                    right
                } else {
                    left
                },
            ))
        }
        (value @ Some(_), None) | (None, value @ Some(_)) => Ok(value),
        (None, None) => Ok(None),
    }
}

fn array_scalar_bounds(
    array: &ArrayRef,
) -> Result<Vec<StatisticsScalarBound>, DistributedQueryError> {
    macro_rules! values {
        ($array:expr) => {
            return Ok($array
                .iter()
                .flatten()
                .map(|value| StatisticsScalarBound::F64(value as f64))
                .collect())
        };
    }
    if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
        values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
        values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
        values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
        values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(array
            .iter()
            .flatten()
            .map(|value| StatisticsScalarBound::F64(value as f64))
            .collect());
    }
    if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
        return array
            .iter()
            .flatten()
            .map(|value| {
                let value = value as f64;
                value
                    .is_finite()
                    .then_some(StatisticsScalarBound::F64(value))
                    .ok_or_else(|| contract_violation("statistics numeric value is not finite"))
            })
            .collect();
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        return array
            .iter()
            .flatten()
            .map(|value| {
                value
                    .is_finite()
                    .then_some(StatisticsScalarBound::F64(value))
                    .ok_or_else(|| contract_violation("statistics numeric value is not finite"))
            })
            .collect();
    }
    if let Some(array) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        if array.value_length() != novarocks_types::largeint::LARGEINT_BYTE_WIDTH {
            return Ok(Vec::new());
        }
        return array
            .iter()
            .flatten()
            .map(|value| {
                novarocks_types::largeint::i128_from_be_bytes(value)
                    .map(StatisticsScalarBound::LargeInt)
                    .map_err(|error| {
                        contract_violation(format!("statistics LARGEINT value: {error}"))
                    })
            })
            .collect();
    }
    Ok(Vec::new())
}

fn array_hashes(array: &ArrayRef) -> Result<Vec<u64>, DistributedQueryError> {
    let mut values = Vec::new();
    macro_rules! hash_values {
        ($array:expr) => {
            for value in $array.iter().flatten() {
                values.push(statistics_value_hash(&value.to_be_bytes()));
            }
            return Ok(values);
        };
    }
    if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        for value in array.iter().flatten() {
            values.push(statistics_value_hash(value.as_bytes()));
        }
        return Ok(values);
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        for value in array.iter().flatten() {
            values.push(statistics_value_hash(value.as_bytes()));
        }
        return Ok(values);
    }
    if let Some(array) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        for value in array.iter().flatten() {
            values.push(statistics_value_hash(value));
        }
        return Ok(values);
    }
    Err(contract_violation(
        "statistics Theta collection does not support the requested Arrow type",
    ))
}

fn statistics_value_hash(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 digest has at least eight bytes"),
    )
}

/// Typed finalization input for a visible-row collection. The table scalar is
/// used only for ROW_COUNT; every column owns an independent scalar partial.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StatisticsCollectionFinalizer {
    table: Option<StatisticsScalarPartial>,
    columns: BTreeMap<std::sync::Arc<str>, StatisticsScalarPartial>,
    theta: BTreeMap<std::sync::Arc<str>, ThetaSketchPartial>,
}

impl StatisticsCollectionFinalizer {
    pub fn with_table(mut self, partial: StatisticsScalarPartial) -> Self {
        self.table = Some(partial);
        self
    }

    pub fn with_column(
        mut self,
        column: impl Into<std::sync::Arc<str>>,
        partial: StatisticsScalarPartial,
    ) -> Self {
        self.columns.insert(column.into(), partial);
        self
    }

    pub fn with_theta(
        mut self,
        column: impl Into<std::sync::Arc<str>>,
        partial: ThetaSketchPartial,
    ) -> Self {
        self.theta.insert(column.into(), partial);
        self
    }

    /// Merge independently collected fragment partials before constructing
    /// connector evidence.  The frontend never sees Arrow rows: it receives
    /// only this bounded associative state through final execution reports.
    pub fn try_merge(
        partials: impl IntoIterator<Item = Self>,
    ) -> Result<Self, DistributedQueryError> {
        let partials = partials.into_iter().collect::<Vec<_>>();
        let table = StatisticsScalarPartial::try_merge(
            partials.iter().filter_map(|partial| partial.table.clone()),
        )?;
        let has_table = partials.iter().any(|partial| partial.table.is_some());
        let mut column_partials =
            BTreeMap::<std::sync::Arc<str>, Vec<StatisticsScalarPartial>>::new();
        let mut theta_partials = BTreeMap::<std::sync::Arc<str>, Vec<ThetaSketchPartial>>::new();
        for partial in partials {
            for (column, scalar) in partial.columns {
                column_partials.entry(column).or_default().push(scalar);
            }
            for (column, theta) in partial.theta {
                theta_partials.entry(column).or_default().push(theta);
            }
        }
        let columns = column_partials
            .into_iter()
            .map(|(column, partials)| {
                StatisticsScalarPartial::try_merge(partials).map(|value| (column, value))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let theta = theta_partials
            .into_iter()
            .map(|(column, partials)| {
                ThetaSketchPartial::try_union(partials).map(|value| (column, value))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(Self {
            table: has_table.then_some(table),
            columns,
            theta,
        })
    }

    /// Encode a bounded fragment report payload for `ExecStatusReport`.
    /// The payload contains no evidence revision, operation ID, credentials,
    /// or client result rows; those remain frontend/control-plane concerns.
    pub fn try_to_fragment_payload(&self) -> Result<Bytes, DistributedQueryError> {
        let mut bytes = Vec::new();
        bytes.push(STATISTICS_FRAGMENT_PAYLOAD_VERSION);
        match &self.table {
            Some(table) => {
                bytes.push(1);
                encode_scalar_partial(&mut bytes, table);
            }
            None => bytes.push(0),
        }
        encode_scalar_partials(&mut bytes, &self.columns)?;
        encode_theta_partials(&mut bytes, &self.theta)?;
        if bytes.len() > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err(resource_exhausted(
                "statistics fragment report exceeds the SPI payload limit",
            ));
        }
        Ok(Bytes::from(bytes))
    }

    /// Decode a native final-report payload, applying every structural and
    /// payload bound before it can enter coordinator state.
    pub fn try_from_fragment_payload(bytes: &[u8]) -> Result<Self, DistributedQueryError> {
        if bytes.len() > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err(resource_exhausted(
                "statistics fragment report exceeds the SPI payload limit",
            ));
        }
        let mut cursor = 0usize;
        let version = take_bytes(bytes, &mut cursor, 1)?[0];
        if version != STATISTICS_FRAGMENT_PAYLOAD_VERSION {
            return Err(contract_violation(
                "statistics fragment report has an unsupported version",
            ));
        }
        let table = match take_bytes(bytes, &mut cursor, 1)?[0] {
            0 => None,
            1 => Some(decode_scalar_partial(bytes, &mut cursor)?),
            _ => {
                return Err(contract_violation(
                    "statistics fragment report has an invalid table flag",
                ));
            }
        };
        let columns = decode_scalar_partials(bytes, &mut cursor)?;
        let theta = decode_theta_partials(bytes, &mut cursor)?;
        if cursor != bytes.len() {
            return Err(contract_violation(
                "statistics fragment report has trailing bytes",
            ));
        }
        Ok(Self {
            table,
            columns,
            theta,
        })
    }

    /// Labels each collected value with the per-metric facts of a visible-row
    /// scan: the scan measured `data_version` itself, so every value's basis is
    /// the queried version. Only the numeric nature varies — a Theta sketch
    /// estimates in both directions no matter how completely it was fed.
    pub fn metric_states(
        &self,
        metrics: &StatisticsMetricRequest,
        data_version: &StatisticsDataVersion,
    ) -> BTreeMap<StatisticsMetric, StatisticsMetricState> {
        metrics
            .metrics()
            .iter()
            .cloned()
            .map(|metric| {
                let observed = match &metric {
                    StatisticsMetric::RowCount => self.table.as_ref().and_then(|partial| {
                        partial
                            .metric_values([metric.clone()])
                            .ok()?
                            .remove(&metric)
                    }),
                    StatisticsMetric::NullCount { column }
                    | StatisticsMetric::Minimum { column }
                    | StatisticsMetric::Maximum { column }
                    | StatisticsMetric::AverageSize { column } => {
                        self.columns.get(column).and_then(|partial| {
                            partial
                                .metric_values([metric.clone()])
                                .ok()?
                                .remove(&metric)
                        })
                    }
                    StatisticsMetric::ThetaNdv { column } => self
                        .theta
                        .get(column)
                        .map(|partial| StatisticsMetricValue::F64(partial.finalize().estimate())),
                };
                let state = observed
                    .map(|value| {
                        StatisticsMetricState::Available(StatisticsMetricObservation::new(
                            value,
                            data_version.clone(),
                            StatisticsMetricSource::VisibleRowScan,
                            visible_row_numeric_nature(&metric),
                            StatisticsBasisRelation::Identical,
                        ))
                    })
                    .unwrap_or_else(|| not_collected(&metric));
                (metric.clone(), state)
            })
            .collect()
    }

    /// Encode the exact, mergeable NDV states which an external statistics
    /// publisher needs in addition to the user-visible scalar evidence.  This
    /// is a versioned Core artifact, not an Iceberg payload: providers may
    /// validate and consume it only after they have checked the same pinned
    /// data version used by the scan.
    pub fn try_visible_row_artifact(
        &self,
        data_version: &StatisticsDataVersion,
    ) -> Result<Bytes, DistributedQueryError> {
        let version = data_version.as_bytes();
        let version_len = u16::try_from(version.len())
            .map_err(|_| resource_exhausted("statistics data version is too large"))?;
        let count = u16::try_from(self.theta.len())
            .map_err(|_| resource_exhausted("statistics artifact has too many Theta columns"))?;
        let mut bytes = Vec::with_capacity(1 + 2 + version.len() + 2);
        bytes.push(VISIBLE_ROW_ARTIFACT_VERSION);
        bytes.extend_from_slice(&version_len.to_be_bytes());
        bytes.extend_from_slice(version);
        bytes.extend_from_slice(&count.to_be_bytes());
        for (column, partial) in &self.theta {
            let column = column.as_bytes();
            let column_len = u16::try_from(column.len())
                .map_err(|_| resource_exhausted("statistics artifact column name is too large"))?;
            let theta = partial.to_wire_bytes();
            let theta_len = u32::try_from(theta.len())
                .map_err(|_| resource_exhausted("statistics Theta artifact is too large"))?;
            bytes.extend_from_slice(&column_len.to_be_bytes());
            bytes.extend_from_slice(column);
            bytes.extend_from_slice(&theta_len.to_be_bytes());
            bytes.extend_from_slice(&theta);
        }
        if bytes.len() > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err(resource_exhausted(
                "statistics visible-row artifact exceeds the SPI payload limit",
            ));
        }
        Ok(Bytes::from(bytes))
    }

    /// Finalize one visible-row collection into the exact typed result handed
    /// to a connector publisher.  The method owns the relationship between
    /// evidence and artifact so a distributed executor cannot accidentally
    /// publish a sketch under another data-version or metric selection.
    pub fn finish_visible_row(
        &self,
        data_version: StatisticsDataVersion,
        evidence_revision: StatisticsEvidenceRevision,
        metrics: &StatisticsMetricRequest,
    ) -> Result<StatisticsCollectionResult, DistributedQueryError> {
        let metric_states = self.metric_states(metrics, &data_version);
        if metric_states
            .values()
            .any(|state| !matches!(state, StatisticsMetricState::Available(_)))
        {
            return Err(contract_violation(
                "visible-row statistics collection did not produce every requested metric",
            ));
        }
        let artifact = self.try_visible_row_artifact(&data_version)?;
        // The scan read every visible row, which is what makes this
        // publishable. It is not a claim that every number is exact: the Theta
        // sketch above never is.
        let evidence = StatisticsEvidence::try_new(
            data_version,
            evidence_revision,
            StatisticsRowCoverage::AllVisibleRows,
            metric_states,
        )
        .map_err(|error| {
            contract_violation(format!("assemble statistics collection evidence: {error}"))
        })?;
        StatisticsCollectionResult::try_new(evidence, artifact).map_err(|error| {
            contract_violation(format!("encode statistics collection result: {error}"))
        })
    }
}

/// A visible-row scan reads every live row, so its counts and bounds are exact
/// on the version it measured. A Theta sketch is a two-sided estimate no matter
/// how complete its input was.
fn visible_row_numeric_nature(metric: &StatisticsMetric) -> StatisticsNumericNature {
    match metric {
        StatisticsMetric::ThetaNdv { .. } => StatisticsNumericNature::TwoSidedApproximate,
        StatisticsMetric::RowCount
        | StatisticsMetric::NullCount { .. }
        | StatisticsMetric::Minimum { .. }
        | StatisticsMetric::Maximum { .. }
        | StatisticsMetric::AverageSize { .. } => StatisticsNumericNature::Exact,
    }
}

/// Decode the provider-neutral visible-row artifact.  It is public within the
/// Core crate because the Iceberg provider is still hosted here until SPI-5;
/// it deliberately is not exposed as a connector-specific wire type.
pub(crate) fn decode_visible_row_artifact(
    bytes: &[u8],
) -> Result<
    (
        StatisticsDataVersion,
        BTreeMap<std::sync::Arc<str>, ThetaSketchPartial>,
    ),
    DistributedQueryError,
> {
    let mut cursor = 0usize;
    let version = take_bytes(bytes, &mut cursor, 1)?[0];
    if version != VISIBLE_ROW_ARTIFACT_VERSION {
        return Err(contract_violation(
            "statistics visible-row artifact has an unsupported version",
        ));
    }
    let data_version_len = u16::from_be_bytes(
        take_bytes(bytes, &mut cursor, 2)?
            .try_into()
            .expect("fixed artifact field width"),
    ) as usize;
    let data_version = StatisticsDataVersion::try_new(Bytes::copy_from_slice(take_bytes(
        bytes,
        &mut cursor,
        data_version_len,
    )?))
    .map_err(|error| contract_violation(format!("decode statistics data version: {error}")))?;
    let count = u16::from_be_bytes(
        take_bytes(bytes, &mut cursor, 2)?
            .try_into()
            .expect("fixed artifact field width"),
    ) as usize;
    let mut theta = BTreeMap::new();
    for _ in 0..count {
        let column_len = u16::from_be_bytes(
            take_bytes(bytes, &mut cursor, 2)?
                .try_into()
                .expect("fixed artifact field width"),
        ) as usize;
        let column = std::str::from_utf8(take_bytes(bytes, &mut cursor, column_len)?)
            .map_err(|_| contract_violation("statistics artifact column is not UTF-8"))?;
        if column.is_empty() {
            return Err(contract_violation(
                "statistics artifact has an empty column name",
            ));
        }
        let theta_len = u32::from_be_bytes(
            take_bytes(bytes, &mut cursor, 4)?
                .try_into()
                .expect("fixed artifact field width"),
        ) as usize;
        let partial =
            ThetaSketchPartial::try_from_wire_bytes(take_bytes(bytes, &mut cursor, theta_len)?)?;
        if theta
            .insert(std::sync::Arc::from(column), partial)
            .is_some()
        {
            return Err(contract_violation(
                "statistics artifact contains duplicate Theta column",
            ));
        }
    }
    if cursor != bytes.len() {
        return Err(contract_violation(
            "statistics visible-row artifact has trailing bytes",
        ));
    }
    Ok((data_version, theta))
}

fn take_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    count: usize,
) -> Result<&'a [u8], DistributedQueryError> {
    let end = cursor
        .checked_add(count)
        .ok_or_else(|| contract_violation("statistics artifact length overflow"))?;
    let output = bytes
        .get(*cursor..end)
        .ok_or_else(|| contract_violation("statistics visible-row artifact is truncated"))?;
    *cursor = end;
    Ok(output)
}

fn encode_scalar_partial(bytes: &mut Vec<u8>, partial: &StatisticsScalarPartial) {
    bytes.extend_from_slice(&partial.row_count.to_be_bytes());
    bytes.extend_from_slice(&partial.null_count.to_be_bytes());
    bytes.extend_from_slice(&partial.total_size.to_be_bytes());
    for value in [&partial.minimum, &partial.maximum] {
        match value {
            Some(StatisticsScalarBound::F64(value)) => {
                bytes.push(1);
                bytes.extend_from_slice(&value.to_bits().to_be_bytes());
            }
            Some(StatisticsScalarBound::LargeInt(value)) => {
                bytes.push(2);
                bytes.extend_from_slice(&value.to_be_bytes());
            }
            None => bytes.push(0),
        }
    }
}

fn decode_scalar_partial(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<StatisticsScalarPartial, DistributedQueryError> {
    let read_u64 = |cursor: &mut usize| -> Result<u64, DistributedQueryError> {
        Ok(u64::from_be_bytes(
            take_bytes(bytes, cursor, 8)?
                .try_into()
                .expect("fixed scalar field width"),
        ))
    };
    let row_count = read_u64(cursor)?;
    let null_count = read_u64(cursor)?;
    let total_size = read_u64(cursor)?;
    let read_bound =
        |cursor: &mut usize| -> Result<Option<StatisticsScalarBound>, DistributedQueryError> {
            match take_bytes(bytes, cursor, 1)?[0] {
                0 => Ok(None),
                1 => Ok(Some(StatisticsScalarBound::F64(f64::from_bits(
                    u64::from_be_bytes(
                        take_bytes(bytes, cursor, 8)?
                            .try_into()
                            .expect("fixed scalar field width"),
                    ),
                )))),
                2 => Ok(Some(StatisticsScalarBound::LargeInt(i128::from_be_bytes(
                    take_bytes(bytes, cursor, 16)?
                        .try_into()
                        .expect("fixed LARGEINT scalar field width"),
                )))),
                _ => Err(contract_violation(
                    "statistics scalar partial has an invalid bound flag",
                )),
            }
        };
    StatisticsScalarPartial::try_new_bounds(
        row_count,
        null_count,
        total_size,
        read_bound(cursor)?,
        read_bound(cursor)?,
    )
}

fn encode_scalar_partials(
    bytes: &mut Vec<u8>,
    partials: &BTreeMap<std::sync::Arc<str>, StatisticsScalarPartial>,
) -> Result<(), DistributedQueryError> {
    let count = u16::try_from(partials.len()).map_err(|_| {
        resource_exhausted("statistics fragment report has too many scalar columns")
    })?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (column, partial) in partials {
        encode_fragment_column(bytes, column)?;
        encode_scalar_partial(bytes, partial);
    }
    Ok(())
}

fn decode_scalar_partials(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<BTreeMap<std::sync::Arc<str>, StatisticsScalarPartial>, DistributedQueryError> {
    let count = u16::from_be_bytes(
        take_bytes(bytes, cursor, 2)?
            .try_into()
            .expect("fixed count width"),
    ) as usize;
    let mut partials = BTreeMap::new();
    for _ in 0..count {
        let column = decode_fragment_column(bytes, cursor)?;
        let value = decode_scalar_partial(bytes, cursor)?;
        if partials.insert(column, value).is_some() {
            return Err(contract_violation(
                "statistics fragment report has duplicate scalar columns",
            ));
        }
    }
    Ok(partials)
}

fn encode_theta_partials(
    bytes: &mut Vec<u8>,
    partials: &BTreeMap<std::sync::Arc<str>, ThetaSketchPartial>,
) -> Result<(), DistributedQueryError> {
    let count = u16::try_from(partials.len())
        .map_err(|_| resource_exhausted("statistics fragment report has too many Theta columns"))?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (column, partial) in partials {
        encode_fragment_column(bytes, column)?;
        let theta = partial.to_wire_bytes();
        let theta_len = u32::try_from(theta.len())
            .map_err(|_| resource_exhausted("statistics fragment Theta state is too large"))?;
        bytes.extend_from_slice(&theta_len.to_be_bytes());
        bytes.extend_from_slice(&theta);
    }
    Ok(())
}

fn decode_theta_partials(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<BTreeMap<std::sync::Arc<str>, ThetaSketchPartial>, DistributedQueryError> {
    let count = u16::from_be_bytes(
        take_bytes(bytes, cursor, 2)?
            .try_into()
            .expect("fixed count width"),
    ) as usize;
    let mut partials = BTreeMap::new();
    for _ in 0..count {
        let column = decode_fragment_column(bytes, cursor)?;
        let theta_len = u32::from_be_bytes(
            take_bytes(bytes, cursor, 4)?
                .try_into()
                .expect("fixed length width"),
        ) as usize;
        let value = ThetaSketchPartial::try_from_wire_bytes(take_bytes(bytes, cursor, theta_len)?)?;
        if partials.insert(column, value).is_some() {
            return Err(contract_violation(
                "statistics fragment report has duplicate Theta columns",
            ));
        }
    }
    Ok(partials)
}

fn encode_fragment_column(
    bytes: &mut Vec<u8>,
    column: &std::sync::Arc<str>,
) -> Result<(), DistributedQueryError> {
    let column = column.as_bytes();
    let length = u16::try_from(column.len())
        .map_err(|_| resource_exhausted("statistics fragment report column name is too large"))?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(column);
    Ok(())
}

fn decode_fragment_column(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<std::sync::Arc<str>, DistributedQueryError> {
    let length = u16::from_be_bytes(
        take_bytes(bytes, cursor, 2)?
            .try_into()
            .expect("fixed length width"),
    ) as usize;
    let column = std::str::from_utf8(take_bytes(bytes, cursor, length)?)
        .map_err(|_| contract_violation("statistics fragment report column is not UTF-8"))?;
    if column.is_empty() {
        return Err(contract_violation(
            "statistics fragment report has an empty column name",
        ));
    }
    Ok(std::sync::Arc::from(column))
}

fn not_collected(metric: &StatisticsMetric) -> StatisticsMetricState {
    StatisticsMetricState::Missing(StatisticsMissing {
        kind: StatisticsMissingKind::NotCollected,
        message: format!("visible-row collection did not produce metric {metric:?}").into(),
    })
}

impl ThetaSketchPartial {
    /// Build a scalar partial from signed integer values. Collection compilers
    /// use typed Arrow kernels for other input types; this small constructor is
    /// intentionally testable without exposing a SQL function.
    pub fn try_from_i64_values(
        lg_k: u8,
        values: impl IntoIterator<Item = i64>,
    ) -> Result<Self, DistributedQueryError> {
        if !(5..=12).contains(&lg_k) {
            return Err(contract_violation(
                "statistics Theta lg_k must be between 5 and 12",
            ));
        }
        let mut sketch = ThetaSketch::builder().lg_k(lg_k).build();
        for value in values {
            sketch.update(value);
        }
        Self::try_from_sketch(sketch)
    }

    /// Build a Theta partial from canonical value hashes emitted by the
    /// Arrow statistics collector. Hashes remain internal; no SQL aggregate
    /// surface exposes this representation.
    pub fn try_from_hashes(
        lg_k: u8,
        hashes: impl IntoIterator<Item = u64>,
    ) -> Result<Self, DistributedQueryError> {
        if !(5..=12).contains(&lg_k) {
            return Err(contract_violation(
                "statistics Theta lg_k must be between 5 and 12",
            ));
        }
        let mut sketch = ThetaSketch::builder().lg_k(lg_k).build();
        for hash in hashes {
            sketch.update(hash as i64);
        }
        Self::try_from_sketch(sketch)
    }

    fn try_from_sketch(mut sketch: ThetaSketch) -> Result<Self, DistributedQueryError> {
        // The implementation grows its hash table before it reaches nominal
        // capacity. Compact before serializing so the bounded wire contract is
        // independent of how many input batches the fragment processed.
        sketch.trim();
        let lg_k = sketch.lg_k();
        let mut retained_hashes = sketch.iter().collect::<Vec<_>>();
        retained_hashes.sort_unstable();
        if retained_hashes.len() > MAX_STATISTICS_THETA_RETAINED_HASHES {
            return Err(resource_exhausted(
                "statistics Theta partial exceeds the retained-hash limit",
            ));
        }
        Ok(Self {
            lg_k,
            theta: sketch.theta64(),
            retained_hashes,
        })
    }

    /// Union partials from all distributed fragments. The output remains a
    /// partial so a tree-shaped exchange can merge it again before finalizing.
    pub fn try_union(
        partials: impl IntoIterator<Item = Self>,
    ) -> Result<Self, DistributedQueryError> {
        let partials = partials.into_iter().collect::<Vec<_>>();
        let Some(first) = partials.first() else {
            return Self::try_from_i64_values(12, std::iter::empty());
        };
        let lg_k = first.lg_k;
        if partials.iter().any(|partial| partial.lg_k != lg_k) {
            return Err(contract_violation(
                "statistics Theta partials use incompatible lg_k values",
            ));
        }

        let mut theta = partials
            .iter()
            .map(|partial| partial.theta)
            .min()
            .unwrap_or(u64::MAX);
        let mut hashes = BTreeSet::new();
        for partial in partials {
            hashes.extend(
                partial
                    .retained_hashes
                    .into_iter()
                    .filter(|hash| *hash < theta),
            );
        }
        let nominal_entries = 1usize << lg_k;
        if hashes.len() > nominal_entries {
            let cutoff = *hashes
                .iter()
                .nth(nominal_entries)
                .expect("hash set length exceeds the nominal Theta capacity");
            theta = theta.min(cutoff);
            hashes.retain(|hash| *hash < theta);
        }
        if hashes.len() > MAX_STATISTICS_THETA_RETAINED_HASHES {
            return Err(resource_exhausted(
                "statistics Theta union exceeds the retained-hash limit",
            ));
        }
        Ok(Self {
            lg_k,
            theta,
            retained_hashes: hashes.into_iter().collect(),
        })
    }

    pub fn finalize(&self) -> ThetaSketchFinal {
        let fraction = self.theta as f64 / i64::MAX as f64;
        ThetaSketchFinal {
            estimate: if self.retained_hashes.is_empty() {
                0.0
            } else {
                self.retained_hashes.len() as f64 / fraction
            },
        }
    }

    pub(crate) fn compact_parts(&self) -> (u8, u64, Vec<u64>) {
        (self.lg_k, self.theta, self.retained_hashes.clone())
    }

    /// Encode a bounded, deterministic internal wire value for transport from
    /// distributed collection to a connector's opaque publish payload. This is
    /// intentionally not a SQL-visible aggregate representation.
    pub fn to_wire_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            THETA_PARTIAL_WIRE_HEADER_BYTES
                + self.retained_hashes.len() * std::mem::size_of::<u64>(),
        );
        bytes.push(THETA_PARTIAL_WIRE_VERSION);
        bytes.push(self.lg_k);
        bytes.extend_from_slice(&self.theta.to_be_bytes());
        bytes.extend_from_slice(&(self.retained_hashes.len() as u32).to_be_bytes());
        for hash in &self.retained_hashes {
            bytes.extend_from_slice(&hash.to_be_bytes());
        }
        bytes
    }

    /// Decode `to_wire_bytes` and re-apply every collection bound. A corrupt
    /// provider payload must be rejected before it can reach publication.
    pub fn try_from_wire_bytes(bytes: &[u8]) -> Result<Self, DistributedQueryError> {
        if bytes.len() < THETA_PARTIAL_WIRE_HEADER_BYTES {
            return Err(contract_violation(
                "statistics Theta wire state is truncated",
            ));
        }
        if bytes[0] != THETA_PARTIAL_WIRE_VERSION {
            return Err(contract_violation(
                "statistics Theta wire state has an unsupported version",
            ));
        }
        let lg_k = bytes[1];
        if !(5..=12).contains(&lg_k) {
            return Err(contract_violation(
                "statistics Theta wire state has an invalid lg_k",
            ));
        }
        let theta = u64::from_be_bytes(bytes[2..10].try_into().expect("slice width checked"));
        let count =
            u32::from_be_bytes(bytes[10..14].try_into().expect("slice width checked")) as usize;
        if count > MAX_STATISTICS_THETA_RETAINED_HASHES {
            return Err(resource_exhausted(
                "statistics Theta wire state exceeds the retained-hash limit",
            ));
        }
        let expected = THETA_PARTIAL_WIRE_HEADER_BYTES
            .checked_add(
                count
                    .checked_mul(std::mem::size_of::<u64>())
                    .ok_or_else(|| {
                        resource_exhausted("statistics Theta wire state length overflow")
                    })?,
            )
            .ok_or_else(|| resource_exhausted("statistics Theta wire state length overflow"))?;
        if bytes.len() != expected {
            return Err(contract_violation(
                "statistics Theta wire state has an invalid length",
            ));
        }
        let retained_hashes = bytes[THETA_PARTIAL_WIRE_HEADER_BYTES..]
            .chunks_exact(std::mem::size_of::<u64>())
            .map(|chunk| u64::from_be_bytes(chunk.try_into().expect("exact chunks")))
            .collect::<Vec<_>>();
        if retained_hashes.windows(2).any(|pair| pair[0] >= pair[1])
            || retained_hashes.iter().any(|hash| *hash >= theta)
        {
            return Err(contract_violation(
                "statistics Theta wire state is not canonical",
            ));
        }
        Ok(Self {
            lg_k,
            theta,
            retained_hashes,
        })
    }
}

impl ThetaSketchFinal {
    pub const fn estimate(self) -> f64 {
        self.estimate
    }
}

fn contract_violation(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

fn resource_exhausted(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Rejected, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::array::{FixedSizeBinaryBuilder, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableHandle,
        StatisticsCollectionPlan,
    };
    use novarocks_sql::test_support::{NativePreparationFixture, native_preparation_plan};

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    /// Stand-in basis for collector tests that only assert on values. The
    /// collector labels every metric with the version it was handed, so any
    /// stable token works here; the labelling itself is asserted separately.
    fn test_data_version() -> StatisticsDataVersion {
        StatisticsDataVersion::try_new(Bytes::from_static(b"collector-data-v1"))
            .expect("data version")
    }

    fn state_value(state: Option<&StatisticsMetricState>) -> Option<&StatisticsMetricValue> {
        match state {
            Some(StatisticsMetricState::Available(observation)) => Some(observation.value()),
            _ => None,
        }
    }

    fn connector_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("valid connector context")
    }

    fn program_for_preparation() -> StatisticsCollectionProgram {
        let table = ConnectorTableHandle::try_new(
            ConnectorInstanceId::parse("statistics-test").expect("instance id"),
            Bytes::from_static(b"pinned-table"),
        )
        .expect("table handle");
        let data_version = StatisticsDataVersion::try_new(Bytes::from_static(b"snapshot-1"))
            .expect("data version");
        let evidence_revision =
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"collection-1"))
                .expect("evidence revision");
        let metrics =
            StatisticsMetricRequest::try_new(vec![StatisticsMetric::RowCount]).expect("metrics");
        let plan = StatisticsCollectionPlan::try_new(
            table,
            data_version,
            evidence_revision,
            metrics,
            Vec::new(),
            Bytes::from_static(b"provider-plan"),
        )
        .expect("collection plan");
        StatisticsCollectionProgram::try_new(
            plan,
            StatisticsExecutionPolicy::try_new(
                StatisticsExecutionMode::DurableJobAttempt,
                Duration::from_secs(60),
            )
            .expect("policy"),
        )
        .expect("program")
    }

    fn prepared_collection_request_for_test() -> PreparedStatisticsCollectionRequest {
        let plan = native_preparation_plan(NativePreparationFixture::ResultOutput)
            .expect("sealed native plan");
        let prepared =
            crate::query_execution::preparation::prepared_fragment_set_for_native_encode_test(
                &plan,
            )
            .expect("prepared native facts");
        let execution = crate::common::admitted_query_context::QueryExecutionContext::new(
            novarocks_types::ClusterRole::AllInOne,
            BackendTopologySnapshot::empty(17),
            Some(Instant::now() + Duration::from_secs(60)),
            crate::common::query_cancellation::QueryCancellationSource::new().view(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
        );
        PreparedStatisticsCollectionRequest {
            encoding: crate::query_execution::post_compile::NativeFragmentEncodingInput::new(
                plan, prepared,
            ),
            program: program_for_preparation(),
            execution,
        }
    }

    fn seal_collection_attachment(
        view: crate::query_execution::native_fragment::NativeFragmentEncodingView<'_>,
    ) -> crate::query_execution::native_fragment::NativeFragmentAttachment {
        let fragments = view.distributed_plan().fragments().iter().map(|fragment| {
            novarocks_protocol::plan::PlanFragment {
                fragment_id: fragment.fragment_id,
                ..Default::default()
            }
        });
        view.seal(fragments)
            .expect("sealed statistics attachment for Core finish contract")
    }

    #[test]
    fn prepared_statistics_request_preserves_typed_program_and_execution_intent() {
        let prepared = prepared_collection_request_for_test();
        let native_attachment = seal_collection_attachment(prepared.encoding_view());
        let request = prepared
            .finish(native_attachment)
            .expect("matching attachment completes statistics request");

        assert_eq!(
            request.intent(),
            crate::query_execution::contract::DistributedQueryIntent::Statistics
        );
        let program = request
            .statistics_program()
            .expect("statistics request retains typed program");
        assert_eq!(
            program.plan().data_version,
            StatisticsDataVersion::try_new(Bytes::from_static(b"snapshot-1"))
                .expect("expected data version")
        );
        assert_eq!(request.topology().revision(), 17);
        assert!(request.deadline().is_some());
    }

    #[test]
    fn prepared_statistics_request_rejects_cross_provenance_attachment() {
        let prepared = prepared_collection_request_for_test();
        let other = prepared_collection_request_for_test();
        let attachment = seal_collection_attachment(other.encoding_view());

        let error = match prepared.finish(attachment) {
            Ok(_) => panic!("attachment from a different preparation must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
        assert_eq!(
            error.message(),
            "native fragment bundle does not match the sealed statistics encoding input"
        );
    }

    #[test]
    fn connector_preparation_rejects_empty_topology_without_local_fallback() {
        let error = match statistics_target_parallelism(&BackendTopologySnapshot::empty(7)) {
            Ok(_) => panic!("statistics collection cannot run without a live backend"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), DistributedQueryErrorKind::Rejected);
        assert!(error.message().contains("at least one live backend"));
    }

    #[test]
    fn spi5b_theta_hashes_fixed_size_binary_values() {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(3, 16);
        builder.append_value([0_u8; 16]).expect("first fixed value");
        builder
            .append_value([1_u8; 16])
            .expect("second fixed value");
        builder.append_null();
        let hashes = array_hashes(&(Arc::new(builder.finish()) as ArrayRef))
            .expect("fixed-size binary values are hashable");

        assert_eq!(hashes.len(), 2);
        assert_ne!(hashes[0], hashes[1]);
    }

    #[test]
    fn theta_wire_roundtrip_preserves_final_estimate() {
        let partial =
            ThetaSketchPartial::try_from_i64_values(12, 0_i64..10_000).expect("build partial");
        let restored = ThetaSketchPartial::try_from_wire_bytes(&partial.to_wire_bytes())
            .expect("decode canonical wire state");
        assert_eq!(restored, partial);
        assert_eq!(restored.finalize(), partial.finalize());
    }

    #[test]
    fn theta_wire_rejects_noncanonical_hashes() {
        let partial =
            ThetaSketchPartial::try_from_i64_values(12, 0_i64..100).expect("build partial");
        let mut bytes = partial.to_wire_bytes();
        if bytes.len() > THETA_PARTIAL_WIRE_HEADER_BYTES + 8 {
            bytes[THETA_PARTIAL_WIRE_HEADER_BYTES..THETA_PARTIAL_WIRE_HEADER_BYTES + 8]
                .copy_from_slice(&u64::MAX.to_be_bytes());
            assert!(ThetaSketchPartial::try_from_wire_bytes(&bytes).is_err());
        }
    }

    #[test]
    fn scalar_partials_merge_row_null_bounds_and_size() {
        let merged = StatisticsScalarPartial::try_merge([
            StatisticsScalarPartial::try_new(3, 1, 30, Some(4.0), Some(9.0))
                .expect("first partial"),
            StatisticsScalarPartial::try_new(2, 0, 10, Some(1.0), Some(7.0))
                .expect("second partial"),
        ])
        .expect("merge partials");
        let values = merged
            .metric_values([
                StatisticsMetric::RowCount,
                StatisticsMetric::NullCount { column: "v".into() },
                StatisticsMetric::Minimum { column: "v".into() },
                StatisticsMetric::Maximum { column: "v".into() },
                StatisticsMetric::AverageSize { column: "v".into() },
            ])
            .expect("metric values");
        assert_eq!(
            values.get(&StatisticsMetric::RowCount),
            Some(&StatisticsMetricValue::U64(5))
        );
        assert_eq!(
            values.get(&StatisticsMetric::Minimum { column: "v".into() }),
            Some(&StatisticsMetricValue::F64(1.0))
        );
        assert_eq!(
            values.get(&StatisticsMetric::Maximum { column: "v".into() }),
            Some(&StatisticsMetricValue::F64(9.0))
        );
    }

    #[test]
    fn fragment_payload_roundtrip_and_merge_preserve_exact_partials() {
        let first = StatisticsCollectionFinalizer::default()
            .with_table(
                StatisticsScalarPartial::try_new(2, 0, 20, None, None).expect("table partial"),
            )
            .with_column(
                "v",
                StatisticsScalarPartial::try_new(2, 1, 8, Some(3.0), Some(7.0))
                    .expect("column partial"),
            )
            .with_theta(
                "v",
                ThetaSketchPartial::try_from_i64_values(12, [1, 2]).expect("theta partial"),
            );
        let second = StatisticsCollectionFinalizer::default()
            .with_table(
                StatisticsScalarPartial::try_new(1, 0, 10, None, None).expect("table partial"),
            )
            .with_column(
                "v",
                StatisticsScalarPartial::try_new(1, 0, 4, Some(1.0), Some(5.0))
                    .expect("column partial"),
            )
            .with_theta(
                "v",
                ThetaSketchPartial::try_from_i64_values(12, [2, 3]).expect("theta partial"),
            );
        let first = StatisticsCollectionFinalizer::try_from_fragment_payload(
            &first
                .try_to_fragment_payload()
                .expect("encode first fragment"),
        )
        .expect("decode first fragment");
        let second = StatisticsCollectionFinalizer::try_from_fragment_payload(
            &second
                .try_to_fragment_payload()
                .expect("encode second fragment"),
        )
        .expect("decode second fragment");
        let merged = StatisticsCollectionFinalizer::try_merge([first, second])
            .expect("merge fragment partials");
        let metrics = StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::RowCount,
            StatisticsMetric::NullCount { column: "v".into() },
            StatisticsMetric::Minimum { column: "v".into() },
            StatisticsMetric::Maximum { column: "v".into() },
            StatisticsMetric::AverageSize { column: "v".into() },
            StatisticsMetric::ThetaNdv { column: "v".into() },
        ])
        .expect("metrics");
        let states = merged.metric_states(&metrics, &test_data_version());
        assert_eq!(
            state_value(states.get(&StatisticsMetric::RowCount)),
            Some(&StatisticsMetricValue::U64(3))
        );
        assert_eq!(
            state_value(states.get(&StatisticsMetric::Minimum { column: "v".into() })),
            Some(&StatisticsMetricValue::F64(1.0))
        );
        assert!(matches!(
            state_value(states.get(&StatisticsMetric::ThetaNdv { column: "v".into() })),
            Some(StatisticsMetricValue::F64(value)) if *value >= 3.0
        ));
    }

    #[test]
    fn fragment_payload_rejects_trailing_bytes() {
        let payload = StatisticsCollectionFinalizer::default()
            .try_to_fragment_payload()
            .expect("encode empty fragment");
        let mut corrupt = payload.to_vec();
        corrupt.push(0);
        assert!(StatisticsCollectionFinalizer::try_from_fragment_payload(&corrupt).is_err());
    }

    #[test]
    fn program_finalizes_fragment_payloads_with_provider_revision() {
        let program = program_for_preparation();
        let payload = StatisticsCollectionFinalizer::default()
            .with_table(
                StatisticsScalarPartial::try_new(4, 0, 32, None, None).expect("table partial"),
            )
            .try_to_fragment_payload()
            .expect("fragment payload");
        let result = program
            .finish_fragment_payloads([payload])
            .expect("final collection result");
        assert_eq!(*result.evidence.data_version(), program.plan().data_version);
        assert_eq!(
            *result.evidence.evidence_revision(),
            *program.plan().evidence_revision()
        );
        assert_eq!(
            state_value(result.evidence.metrics().get(&StatisticsMetric::RowCount)),
            Some(&StatisticsMetricValue::U64(4))
        );
    }

    #[test]
    fn program_rejects_completion_without_fragment_payload() {
        let error = program_for_preparation()
            .finish_fragment_payloads([Bytes::new()])
            .expect_err("missing partial must fail closed");
        assert!(error.message().contains("without a fragment partial"));
    }

    #[test]
    fn batch_collector_accumulates_typed_scalar_and_theta_metrics() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let metrics = StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::RowCount,
            StatisticsMetric::NullCount { column: "v".into() },
            StatisticsMetric::Minimum { column: "v".into() },
            StatisticsMetric::Maximum { column: "v".into() },
            StatisticsMetric::AverageSize { column: "v".into() },
            StatisticsMetric::ThetaNdv { column: "v".into() },
        ])
        .expect("metrics");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![Some(3), None, Some(1)]))],
        )
        .expect("batch");
        let mut collector =
            StatisticsBatchCollector::try_new(schema, metrics.clone()).expect("collector");
        collector.push_batch(&batch).expect("collect batch");
        let states = collector
            .finish()
            .expect("finish collector")
            .metric_states(&metrics, &test_data_version());
        assert_eq!(
            state_value(states.get(&StatisticsMetric::RowCount)),
            Some(&StatisticsMetricValue::U64(3))
        );
        assert_eq!(
            state_value(states.get(&StatisticsMetric::NullCount { column: "v".into() })),
            Some(&StatisticsMetricValue::U64(1))
        );
        assert_eq!(
            state_value(states.get(&StatisticsMetric::Minimum { column: "v".into() })),
            Some(&StatisticsMetricValue::F64(1.0))
        );
        assert_eq!(
            state_value(states.get(&StatisticsMetric::Maximum { column: "v".into() })),
            Some(&StatisticsMetricValue::F64(3.0))
        );
        assert!(matches!(
            state_value(states.get(&StatisticsMetric::AverageSize { column: "v".into() })),
            Some(StatisticsMetricValue::F64(value)) if *value > 0.0
        ));
        assert!(matches!(
            state_value(states.get(&StatisticsMetric::ThetaNdv { column: "v".into() })),
            Some(StatisticsMetricValue::F64(value)) if *value >= 2.0
        ));

        // Same scan, same basis, different numeric nature: a counted value is
        // exact while the sketch beside it is not.
        let nature = |metric: StatisticsMetric| match states.get(&metric) {
            Some(StatisticsMetricState::Available(observation)) => observation.numeric_nature(),
            other => panic!("expected an available metric, got {other:?}"),
        };
        assert_eq!(
            nature(StatisticsMetric::RowCount),
            StatisticsNumericNature::Exact
        );
        assert_eq!(
            nature(StatisticsMetric::ThetaNdv { column: "v".into() }),
            StatisticsNumericNature::TwoSidedApproximate
        );
    }

    #[test]
    fn spi5b_batch_collector_preserves_largeint_bounds_through_fragment_wire() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH),
            false,
        )]));
        let metrics = StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::Minimum { column: "k".into() },
            StatisticsMetric::Maximum { column: "k".into() },
            StatisticsMetric::ThetaNdv { column: "k".into() },
        ])
        .expect("metrics");
        let mut values = FixedSizeBinaryBuilder::with_capacity(
            3,
            novarocks_types::largeint::LARGEINT_BYTE_WIDTH,
        );
        values
            .append_value(i128::MIN.to_be_bytes())
            .expect("minimum LARGEINT");
        values
            .append_value(0_i128.to_be_bytes())
            .expect("zero LARGEINT");
        values
            .append_value(i128::MAX.to_be_bytes())
            .expect("maximum LARGEINT");
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values.finish())])
            .expect("batch");
        let mut collector =
            StatisticsBatchCollector::try_new(schema, metrics.clone()).expect("collector");
        collector.push_batch(&batch).expect("collect batch");
        let finalizer = collector.finish().expect("finish collector");
        let payload = finalizer
            .try_to_fragment_payload()
            .expect("encode LARGEINT fragment");
        let restored = StatisticsCollectionFinalizer::try_from_fragment_payload(&payload)
            .expect("decode LARGEINT fragment");
        let states = restored.metric_states(&metrics, &test_data_version());

        assert_eq!(
            state_value(states.get(&StatisticsMetric::Minimum { column: "k".into() })),
            Some(&StatisticsMetricValue::Bytes(Bytes::copy_from_slice(
                &i128::MIN.to_be_bytes()
            )))
        );
        assert_eq!(
            state_value(states.get(&StatisticsMetric::Maximum { column: "k".into() })),
            Some(&StatisticsMetricValue::Bytes(Bytes::copy_from_slice(
                &i128::MAX.to_be_bytes()
            )))
        );
    }

    #[test]
    fn batch_collector_rejects_metric_column_absent_from_pinned_schema() {
        let schema = Arc::new(Schema::empty());
        let metrics = StatisticsMetricRequest::try_new(vec![StatisticsMetric::ThetaNdv {
            column: "missing".into(),
        }])
        .expect("metrics");
        let error = match StatisticsBatchCollector::try_new(schema, metrics) {
            Ok(_) => panic!("missing projection must fail closed"),
            Err(error) => error,
        };
        assert!(
            error
                .message()
                .contains("does not contain requested column")
        );
    }

    #[test]
    fn batch_collector_keeps_theta_state_bounded_across_many_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let metrics = StatisticsMetricRequest::try_new(vec![StatisticsMetric::ThetaNdv {
            column: "v".into(),
        }])
        .expect("metrics");
        let mut collector =
            StatisticsBatchCollector::try_new(schema.clone(), metrics.clone()).expect("collector");
        for start in (0_i64..10_000).step_by(100) {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(
                    (start..start + 100).map(Some).collect::<Vec<_>>(),
                ))],
            )
            .expect("batch");
            collector.push_batch(&batch).expect("collect batch");
        }
        let states = collector
            .finish()
            .expect("finish collector")
            .metric_states(&metrics, &test_data_version());
        assert!(matches!(
            state_value(states.get(&StatisticsMetric::ThetaNdv { column: "v".into() })),
            Some(StatisticsMetricValue::F64(value)) if *value > 9_000.0
        ));
    }

    #[test]
    fn finalizer_preserves_missing_metrics_instead_of_fabricating_values() {
        let metrics = StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::RowCount,
            StatisticsMetric::ThetaNdv {
                column: "missing".into(),
            },
        ])
        .expect("metrics");
        let states = StatisticsCollectionFinalizer::default()
            .with_table(StatisticsScalarPartial::try_new(3, 0, 12, None, None).expect("scalar"))
            .metric_states(&metrics, &test_data_version());
        assert!(matches!(
            state_value(states.get(&StatisticsMetric::RowCount)),
            Some(StatisticsMetricValue::U64(3))
        ));
        assert!(matches!(
            states.get(&StatisticsMetric::ThetaNdv {
                column: "missing".into()
            }),
            Some(StatisticsMetricState::Missing(StatisticsMissing {
                kind: StatisticsMissingKind::NotCollected,
                ..
            }))
        ));
    }

    #[test]
    fn visible_row_artifact_round_trips_the_pinned_version_and_theta_state() {
        let data_version =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table/v1")).expect("version");
        let partial =
            ThetaSketchPartial::try_from_i64_values(12, 0_i64..1_000).expect("theta partial");
        let artifact = StatisticsCollectionFinalizer::default()
            .with_theta("customer_id", partial.clone())
            .try_visible_row_artifact(&data_version)
            .expect("encode artifact");
        let (decoded_version, theta) = decode_visible_row_artifact(&artifact).expect("decode");
        assert_eq!(decoded_version, data_version);
        assert_eq!(theta.get("customer_id"), Some(&partial));
    }

    #[test]
    fn visible_row_artifact_rejects_trailing_bytes() {
        let data_version =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table/v1")).expect("version");
        let mut artifact = StatisticsCollectionFinalizer::default()
            .try_visible_row_artifact(&data_version)
            .expect("encode artifact")
            .to_vec();
        artifact.push(1);
        assert!(decode_visible_row_artifact(&artifact).is_err());
    }

    #[test]
    fn finalizer_binds_visible_row_evidence_to_the_artifact_version() {
        let data_version =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table/v1")).expect("version");
        let revision =
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"run/v1")).expect("revision");
        let metrics = StatisticsMetricRequest::try_new(vec![StatisticsMetric::ThetaNdv {
            column: "customer_id".into(),
        }])
        .expect("metrics");
        let result = StatisticsCollectionFinalizer::default()
            .with_theta(
                "customer_id",
                ThetaSketchPartial::try_from_i64_values(12, 0_i64..100).expect("theta"),
            )
            .finish_visible_row(data_version.clone(), revision.clone(), &metrics)
            .expect("finalize");
        assert_eq!(*result.evidence.data_version(), data_version);
        assert_eq!(*result.evidence.evidence_revision(), revision);
        assert_eq!(
            result.evidence.row_coverage(),
            StatisticsRowCoverage::AllVisibleRows
        );
        // The scan saw every visible row, but its Theta sketch is still an
        // estimate. Coverage and numeric nature must be able to say both.
        let ndv = match result.evidence.metrics().get(&StatisticsMetric::ThetaNdv {
            column: "customer_id".into(),
        }) {
            Some(StatisticsMetricState::Available(observation)) => observation.clone(),
            other => panic!("expected an available NDV, got {other:?}"),
        };
        assert_eq!(*ndv.source(), StatisticsMetricSource::VisibleRowScan);
        assert_eq!(
            ndv.numeric_nature(),
            StatisticsNumericNature::TwoSidedApproximate
        );
        assert_eq!(ndv.basis_relation(), StatisticsBasisRelation::Identical);
        assert_eq!(*ndv.basis_version(), data_version);
        assert_eq!(
            decode_visible_row_artifact(result.provider_payload())
                .expect("decode artifact")
                .0,
            *result.evidence.data_version()
        );
    }
}
