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

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use datasketches::theta::ThetaSketch;
use novarocks_spi::connector::{
    StatisticsCollectionPlan, StatisticsCollectionResult, StatisticsDataVersion,
    StatisticsEvidence, StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricRequest,
    StatisticsMetricState, StatisticsMetricValue, StatisticsMissing, StatisticsMissingKind,
    StatisticsProvenance,
};

use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};

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
        if evidence.data_version != self.data_version {
            return Err(contract_violation(
                "statistics collection result does not match its pinned data version",
            ));
        }
        let expected = self.metrics.metrics().iter().collect::<BTreeSet<_>>();
        let actual = evidence.metrics.keys().collect::<BTreeSet<_>>();
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

/// Mergeable scalar partial for row/null/min/max/size collection. Numeric
/// bounds are deliberately typed at the compiler boundary; unsupported input
/// types must be reported as missing rather than coerced through strings.
#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsScalarPartial {
    row_count: u64,
    null_count: u64,
    total_size: u64,
    minimum: Option<f64>,
    maximum: Option<f64>,
}

impl StatisticsScalarPartial {
    pub fn try_new(
        row_count: u64,
        null_count: u64,
        total_size: u64,
        minimum: Option<f64>,
        maximum: Option<f64>,
    ) -> Result<Self, DistributedQueryError> {
        if null_count > row_count {
            return Err(contract_violation(
                "statistics null count exceeds row count",
            ));
        }
        if minimum.is_some_and(|value| !value.is_finite())
            || maximum.is_some_and(|value| !value.is_finite())
            || matches!((minimum, maximum), (Some(minimum), Some(maximum)) if minimum > maximum)
        {
            return Err(contract_violation(
                "statistics scalar bounds must be finite and ordered",
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
        let mut merged = Self::try_new(0, 0, 0, None, None)?;
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
            merged.minimum = match (merged.minimum, partial.minimum) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (value @ Some(_), None) | (None, value @ Some(_)) => value,
                (None, None) => None,
            };
            merged.maximum = match (merged.maximum, partial.maximum) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (value @ Some(_), None) | (None, value @ Some(_)) => value,
                (None, None) => None,
            };
        }
        Self::try_new(
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
    ) -> Result<BTreeMap<StatisticsMetric, StatisticsMetricState>, DistributedQueryError> {
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
                StatisticsMetric::Minimum { .. } => StatisticsMetricValue::F64(
                    self.minimum
                        .ok_or_else(|| contract_violation("statistics minimum is unavailable"))?,
                ),
                StatisticsMetric::Maximum { .. } => StatisticsMetricValue::F64(
                    self.maximum
                        .ok_or_else(|| contract_violation("statistics maximum is unavailable"))?,
                ),
                StatisticsMetric::ThetaNdv { .. } => continue,
            };
            output.insert(metric, StatisticsMetricState::Available(value));
        }
        Ok(output)
    }
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

    pub fn metric_states(
        &self,
        metrics: &StatisticsMetricRequest,
    ) -> BTreeMap<StatisticsMetric, StatisticsMetricState> {
        metrics
            .metrics()
            .iter()
            .cloned()
            .map(|metric| {
                let state = match &metric {
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
                    StatisticsMetric::ThetaNdv { column } => {
                        self.theta.get(column).map(|partial| {
                            StatisticsMetricState::Available(StatisticsMetricValue::F64(
                                partial.finalize().estimate(),
                            ))
                        })
                    }
                };
                (
                    metric.clone(),
                    state.unwrap_or_else(|| not_collected(&metric)),
                )
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
        let metric_states = self.metric_states(metrics);
        if metric_states
            .values()
            .any(|state| !matches!(state, StatisticsMetricState::Available(_)))
        {
            return Err(contract_violation(
                "visible-row statistics collection did not produce every requested metric",
            ));
        }
        let artifact = self.try_visible_row_artifact(&data_version)?;
        StatisticsCollectionResult::try_new(
            StatisticsEvidence {
                data_version,
                evidence_revision,
                coverage: novarocks_spi::connector::StatisticsCoverage::Full,
                accuracy: novarocks_spi::connector::StatisticsAccuracy::Exact,
                interval: None,
                provenance: StatisticsProvenance::VisibleRows,
                metrics: metric_states,
            },
            artifact,
        )
        .map_err(|error| {
            contract_violation(format!("encode statistics collection result: {error}"))
        })
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
    use super::*;

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
            Some(&StatisticsMetricState::Available(
                StatisticsMetricValue::U64(5)
            ))
        );
        assert_eq!(
            values.get(&StatisticsMetric::Minimum { column: "v".into() }),
            Some(&StatisticsMetricState::Available(
                StatisticsMetricValue::F64(1.0)
            ))
        );
        assert_eq!(
            values.get(&StatisticsMetric::Maximum { column: "v".into() }),
            Some(&StatisticsMetricState::Available(
                StatisticsMetricValue::F64(9.0)
            ))
        );
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
            .metric_states(&metrics);
        assert!(matches!(
            states.get(&StatisticsMetric::RowCount),
            Some(StatisticsMetricState::Available(
                StatisticsMetricValue::U64(3)
            ))
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
        assert_eq!(result.evidence.data_version, data_version);
        assert_eq!(result.evidence.evidence_revision, revision);
        assert_eq!(
            result.evidence.provenance,
            StatisticsProvenance::VisibleRows
        );
        assert_eq!(
            decode_visible_row_artifact(result.provider_payload())
                .expect("decode artifact")
                .0,
            result.evidence.data_version
        );
    }
}
