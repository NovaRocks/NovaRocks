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

use std::collections::BTreeSet;
use std::time::Duration;

use datasketches::theta::ThetaSketch;
use novarocks_spi::connector::{
    StatisticsCollectionPlan, StatisticsCollectionResult, StatisticsMetricRequest,
};

use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};

/// The longest independently owned durable ANALYZE attempt. A client wait
/// deadline is intentionally not an attempt deadline.
pub const MAX_STATISTICS_ATTEMPT_DURATION: Duration = Duration::from_secs(30 * 60);

/// Bound the in-memory, mergeable Theta state produced by one statistics
/// collection. This is independent of the SPI's wire-payload bound.
pub const MAX_STATISTICS_THETA_RETAINED_HASHES: usize = 1 << 16;

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

impl ThetaSketchPartial {
    /// Build a scalar partial from signed integer values. Collection compilers
    /// use typed Arrow kernels for other input types; this small constructor is
    /// intentionally testable without exposing a SQL function.
    pub fn try_from_i64_values(
        lg_k: u8,
        values: impl IntoIterator<Item = i64>,
    ) -> Result<Self, DistributedQueryError> {
        if !(5..=16).contains(&lg_k) {
            return Err(contract_violation(
                "statistics Theta lg_k must be between 5 and 16",
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
