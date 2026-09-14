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

use std::sync::atomic::{AtomicU64, Ordering};

use crate::management_http::RoleMetricsRenderer;
use once_cell::sync::Lazy;
use prometheus::{Encoder, IntCounter, IntGaugeVec, Opts, Registry, TextEncoder};

/// Explicitly-owned Backend metric registry.  It is intentionally separate
/// from Prometheus' process-global registry so an all-in-one process cannot
/// leak a foreign role's metric families through the BE management endpoint.
pub struct BackendMetricsRegistry {
    registry: Registry,
}

impl BackendMetricsRegistry {
    pub fn new() -> Result<Self, String> {
        let registry = Registry::new();
        let collectors = [
            Box::new(Lazy::force(&BACKEND_QUERY_EXECUTION_RESOURCES).clone())
                as Box<dyn prometheus::core::Collector>,
            Box::new(Lazy::force(&BACKEND_TASK_EXECUTION_TASKS_CREATED).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_AUTHENTICATION_FAILURES).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_TLS_FAILURES).clone()),
            Box::new(Lazy::force(&BACKEND_CONNECTOR_WRITE_WRITER_OPENS).clone()),
            Box::new(Lazy::force(&BACKEND_CONNECTOR_WRITE_WRITER_TOTALS).clone()),
            Box::new(Lazy::force(&BACKEND_CONNECTOR_WRITE_WRITER_ABORTS).clone()),
            Box::new(Lazy::force(&BACKEND_CONNECTOR_WRITE_ROOT_SET_PEAK).clone()),
            Box::new(Lazy::force(&BACKEND_FRAGMENT_RESULT_TERMINALS).clone()),
        ];
        for collector in collectors {
            registry
                .register(collector)
                .map_err(|error| format!("register backend metrics collector: {error}"))?;
        }
        #[cfg(debug_assertions)]
        registry
            .register(Box::new(
                Lazy::force(&BACKEND_CONNECTOR_WRITE_DEBUG_FAULTS).clone(),
            ))
            .map_err(|error| format!("register backend debug metrics collector: {error}"))?;
        novarocks_execution::runtime::fragment::io::exchange_metrics::register_exchange_metrics(
            &registry,
        )?;
        novarocks_execution::runtime::table_writer_metrics::register_table_writer_metrics(
            &registry,
        )?;
        Ok(Self { registry })
    }

    fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }
}

static BACKEND_QUERY_EXECUTION_RESOURCES: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_query_execution_resources",
            "Backend execution resources as reported by their owning component.",
        ),
        &["resource"],
    )
    .expect("construct novarocks_backend_query_execution_resources")
});

/// Tasks first accepted by the EES task registry in this backend process.
///
/// A task is the EES-owned replacement for one admitted fragment. Idempotent
/// CreateTask replays do not advance this counter.
static BACKEND_TASK_EXECUTION_TASKS_CREATED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(Opts::new(
        "novarocks_backend_task_execution_tasks_created_total",
        "Cumulative EES tasks first accepted by this backend.",
    ))
    .expect("construct novarocks_backend_task_execution_tasks_created_total")
});

/// Writer opens attempted by this backend's connector write data plane, by
/// outcome. One driver opens exactly one writer, so this counts drivers.
static BACKEND_CONNECTOR_WRITE_WRITER_OPENS: Lazy<prometheus::IntCounterVec> = Lazy::new(|| {
    prometheus::IntCounterVec::new(
        Opts::new(
            "novarocks_backend_connector_write_writer_opens_total",
            "Cumulative connector write writer opens on this backend, by outcome.",
        ),
        &["outcome"],
    )
    .expect("construct novarocks_backend_connector_write_writer_opens_total")
});

/// Rows accepted by finished connector writers on this backend, and the
/// commit fragments those writers produced.
static BACKEND_CONNECTOR_WRITE_WRITER_TOTALS: Lazy<prometheus::IntCounterVec> = Lazy::new(|| {
    prometheus::IntCounterVec::new(
        Opts::new(
            "novarocks_backend_connector_write_writer_totals",
            "Cumulative connector write writer rows and commit fragments on this backend.",
        ),
        &["unit"],
    )
    .expect("construct novarocks_backend_connector_write_writer_totals")
});

/// Provider writer abort calls completed by this backend, by outcome.
///
/// The counter is advanced only after the provider future returns, so
/// `succeeded` is direct evidence that cancellation crossed the adapter and
/// settled at the provider boundary rather than merely suppressing commit.
static BACKEND_CONNECTOR_WRITE_WRITER_ABORTS: Lazy<prometheus::IntCounterVec> = Lazy::new(|| {
    prometheus::IntCounterVec::new(
        Opts::new(
            "novarocks_backend_connector_write_writer_aborts_total",
            "Cumulative completed connector writer abort calls on this backend, by outcome.",
        ),
        &["outcome"],
    )
    .expect("construct novarocks_backend_connector_write_writer_aborts_total")
});

/// Debug-only write faults that reached their exact execution boundary.
#[cfg(debug_assertions)]
static BACKEND_CONNECTOR_WRITE_DEBUG_FAULTS: Lazy<prometheus::IntCounterVec> = Lazy::new(|| {
    prometheus::IntCounterVec::new(
        Opts::new(
            "novarocks_backend_connector_write_debug_faults_total",
            "Cumulative debug-only connector write faults that reached their bound attempt.",
        ),
        &["kind"],
    )
    .expect("construct novarocks_backend_connector_write_debug_faults_total")
});

/// Native fragment result sessions that reached one accepted terminal.
/// `finished` is the owner-side publication of successful Root EOF.
static BACKEND_FRAGMENT_RESULT_TERMINALS: Lazy<prometheus::IntCounterVec> = Lazy::new(|| {
    prometheus::IntCounterVec::new(
        Opts::new(
            "novarocks_backend_fragment_result_terminals_total",
            "Cumulative accepted native fragment result terminals, by terminal.",
        ),
        &["terminal"],
    )
    .expect("construct novarocks_backend_fragment_result_terminals_total")
});

/// High-water mark of the prepared write set observed by a root aggregation on
/// this backend. Bytes and entries are the two frozen budgets the root bounds.
static BACKEND_CONNECTOR_WRITE_ROOT_SET_PEAK: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_connector_write_root_prepared_set_peak",
            "Peak prepared write set observed by a root aggregation on this backend.",
        ),
        &["dimension"],
    )
    .expect("construct novarocks_backend_connector_write_root_prepared_set_peak")
});

static BACKEND_CONNECTOR_WRITE_ROOT_SET_PEAK_BYTES: AtomicU64 = AtomicU64::new(0);
static BACKEND_CONNECTOR_WRITE_ROOT_SET_PEAK_ENTRIES: AtomicU64 = AtomicU64::new(0);

static BACKEND_NATIVE_AUTHENTICATION_FAILURES: Lazy<prometheus::IntCounterVec> = Lazy::new(|| {
    prometheus::IntCounterVec::new(
        Opts::new(
            "novarocks_native_authentication_failures_total",
            "Cumulative rejected Native caller authentication attempts.",
        ),
        &["reason"],
    )
    .expect("construct novarocks_native_authentication_failures_total")
});

static BACKEND_NATIVE_TLS_FAILURES: Lazy<prometheus::IntCounterVec> = Lazy::new(|| {
    prometheus::IntCounterVec::new(
        Opts::new(
            "novarocks_native_tls_failures_total",
            "Cumulative Native TLS handshake failures.",
        ),
        &["phase", "reason"],
    )
    .expect("construct novarocks_native_tls_failures_total")
});

static BACKEND_NATIVE_AUTH_FAILURE_LOG_SAMPLE: AtomicU64 = AtomicU64::new(0);
static BACKEND_NATIVE_TLS_FAILURE_LOG_SAMPLE: AtomicU64 = AtomicU64::new(0);

/// One driver attempted to open its own connector writer. `outcome` is a
/// closed vocabulary: `opened` or `failed`.
pub fn record_connector_write_writer_open(outcome: &'static str) {
    BACKEND_CONNECTOR_WRITE_WRITER_OPENS
        .with_label_values(&[outcome])
        .inc();
}

pub fn record_task_execution_task_created() {
    BACKEND_TASK_EXECUTION_TASKS_CREATED.inc();
}

/// One connector writer finished: it accepted `rows` rows and produced
/// `fragments` commit fragments.
pub fn record_connector_write_writer_finished(rows: u64, fragments: u64) {
    BACKEND_CONNECTOR_WRITE_WRITER_TOTALS
        .with_label_values(&["rows"])
        .inc_by(rows);
    BACKEND_CONNECTOR_WRITE_WRITER_TOTALS
        .with_label_values(&["commit_fragments"])
        .inc_by(fragments);
}

pub fn record_connector_write_writer_abort(outcome: &'static str) {
    BACKEND_CONNECTOR_WRITE_WRITER_ABORTS
        .with_label_values(&[outcome])
        .inc();
}

#[cfg(debug_assertions)]
pub fn record_connector_write_debug_fault(kind: &'static str) {
    BACKEND_CONNECTOR_WRITE_DEBUG_FAULTS
        .with_label_values(&[kind])
        .inc();
}

pub fn record_fragment_result_terminal(terminal: &'static str) {
    BACKEND_FRAGMENT_RESULT_TERMINALS
        .with_label_values(&[terminal])
        .inc();
}

/// Publish the prepared write set a root aggregation has accepted so far. The
/// gauge keeps the process-wide high-water mark, so a later, smaller write
/// never erases the peak an operator needs in order to size the budgets.
pub fn publish_connector_write_root_prepared_set_peak(bytes: u64, entries: u64) {
    for (dimension, value, peak) in [
        ("bytes", bytes, &BACKEND_CONNECTOR_WRITE_ROOT_SET_PEAK_BYTES),
        (
            "entries",
            entries,
            &BACKEND_CONNECTOR_WRITE_ROOT_SET_PEAK_ENTRIES,
        ),
    ] {
        let gauge = BACKEND_CONNECTOR_WRITE_ROOT_SET_PEAK.with_label_values(&[dimension]);
        publish_monotonic_peak(peak, &gauge, value);
    }
}

fn publish_monotonic_peak(peak: &AtomicU64, gauge: &prometheus::IntGauge, value: u64) {
    let value = value.min(i64::MAX as u64);
    let previous = peak.fetch_max(value, Ordering::AcqRel);
    if value > previous {
        // Every successful maximum advance contributes only its delta. Gauge
        // addition is atomic, so concurrent advances telescope to the largest
        // observed value regardless of the order in which the winners publish.
        gauge.add((value - previous) as i64);
    }
}

pub fn record_backend_native_authentication_failure() {
    BACKEND_NATIVE_AUTHENTICATION_FAILURES
        .with_label_values(&["authentication"])
        .inc();
    if BACKEND_NATIVE_AUTH_FAILURE_LOG_SAMPLE
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(64)
    {
        tracing::warn!(
            role = "be",
            reason = "authentication",
            "rejected native caller authentication"
        );
    }
}

pub fn record_backend_native_tls_handshake_failure() {
    BACKEND_NATIVE_TLS_FAILURES
        .with_label_values(&["handshake", "transport_configuration"])
        .inc();
    if BACKEND_NATIVE_TLS_FAILURE_LOG_SAMPLE
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(64)
    {
        tracing::warn!(
            role = "be",
            phase = "handshake",
            reason = "transport_configuration",
            "rejected native TLS handshake"
        );
    }
}

/// Publish a scalar snapshot after its owner has released its own lock. The
/// metrics layer deliberately holds no execution resource references.
pub fn publish_backend_query_execution_resource(resource: &'static str, value: usize) {
    BACKEND_QUERY_EXECUTION_RESOURCES
        .with_label_values(&[resource])
        .set(value as i64);
}

/// Renders only the metric families registered by this Backend role.
pub(crate) fn render_metrics(metrics: &BackendMetricsRegistry) -> Result<String, String> {
    refresh_backend_gauges();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&metrics.gather(), &mut buf)
        .map_err(|e| format!("encode prometheus metrics failed: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("prometheus metrics were not utf-8: {e}"))
}

/// Renders only the role-owned metrics in the existing JSON form.
pub(crate) fn render_metrics_json(metrics: &BackendMetricsRegistry) -> Result<String, String> {
    refresh_backend_gauges();
    let mut rows = Vec::new();
    for family in metrics.gather() {
        for metric in family.get_metric() {
            let mut tags = serde_json::Map::new();
            tags.insert(
                "metric".to_string(),
                serde_json::Value::String(family.get_name().to_string()),
            );
            for label in metric.get_label() {
                tags.insert(
                    label.get_name().to_string(),
                    serde_json::Value::String(label.get_value().to_string()),
                );
            }

            if metric.has_counter() {
                rows.push(serde_json::json!({
                    "tags": tags,
                    "value": metric.get_counter().get_value(),
                }));
            } else if metric.has_gauge() {
                rows.push(serde_json::json!({
                    "tags": tags,
                    "value": metric.get_gauge().get_value(),
                }));
            } else if metric.has_untyped() {
                rows.push(serde_json::json!({
                    "tags": tags,
                    "value": metric.get_untyped().get_value(),
                }));
            } else if metric.has_histogram() {
                let histogram = metric.get_histogram();
                let mut count_tags = tags.clone();
                count_tags.insert(
                    "metric".to_string(),
                    serde_json::Value::String(format!("{}_count", family.get_name())),
                );
                rows.push(serde_json::json!({
                    "tags": count_tags,
                    "value": histogram.get_sample_count(),
                }));
                let mut sum_tags = tags;
                sum_tags.insert(
                    "metric".to_string(),
                    serde_json::Value::String(format!("{}_sum", family.get_name())),
                );
                rows.push(serde_json::json!({
                    "tags": sum_tags,
                    "value": histogram.get_sample_sum(),
                }));
            }
        }
    }
    serde_json::to_string(&rows).map_err(|e| format!("encode metrics json failed: {e}"))
}

impl RoleMetricsRenderer for BackendMetricsRegistry {
    fn render_prometheus(&self) -> Result<String, String> {
        render_metrics(self)
    }

    fn render_json(&self) -> Result<String, String> {
        render_metrics_json(self)
    }
}

fn refresh_backend_gauges() {
    Lazy::force(&BACKEND_QUERY_EXECUTION_RESOURCES);
    ensure_backend_metric_label_families();
}

/// Make the documented BE metric families observable before their first event
/// without resetting values already published by their application owner.
fn ensure_backend_metric_label_families() {
    for resource in ["catalog_query_leases", "catalog_handle_leases"] {
        let _ = BACKEND_QUERY_EXECUTION_RESOURCES.get_metric_with_label_values(&[resource]);
    }
    for outcome in ["succeeded", "failed"] {
        let _ = BACKEND_CONNECTOR_WRITE_WRITER_ABORTS.get_metric_with_label_values(&[outcome]);
    }
    #[cfg(debug_assertions)]
    let _ = BACKEND_CONNECTOR_WRITE_DEBUG_FAULTS.get_metric_with_label_values(&["append_hold"]);
    for terminal in ["finished", "aborted"] {
        let _ = BACKEND_FRAGMENT_RESULT_TERMINALS.get_metric_with_label_values(&[terminal]);
    }
}
#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use prometheus::{IntGauge, Opts, Registry};

    use super::*;

    #[test]
    fn role_registry_excludes_foreign_registry_families() {
        let foreign = Registry::new();
        let foreign_metric = IntGauge::with_opts(Opts::new(
            "novarocks_frontend_only_fixture",
            "A foreign role metric for isolation coverage.",
        ))
        .expect("construct foreign collector");
        foreign
            .register(Box::new(foreign_metric))
            .expect("register foreign collector");

        let backend = BackendMetricsRegistry::new().expect("construct Backend registry");
        let rendered = render_metrics(&backend).expect("render Backend metrics");
        assert!(rendered.contains("novarocks_backend_query_execution_resources"));
        assert!(rendered.contains("novarocks_backend_task_execution_tasks_created_total"));
        assert!(rendered.contains("novarocks_exchange_shuffle_bytes_total"));
        assert!(!rendered.contains("novarocks_frontend_only_fixture"));
    }

    #[test]
    fn prepared_set_peak_is_monotonic_under_concurrent_publication() {
        let peak = Arc::new(AtomicU64::new(0));
        let gauge = Arc::new(
            IntGauge::with_opts(Opts::new(
                "connector_write_root_peak_concurrency_fixture",
                "Concurrent peak publication fixture.",
            ))
            .expect("construct peak fixture"),
        );
        publish_monotonic_peak(&peak, &gauge, 73);

        let values = [72_u64, 1, 37, 1_024, 511, 73, 999, 8];
        let start = Arc::new(Barrier::new(values.len() + 1));
        let threads = values
            .into_iter()
            .map(|value| {
                let peak = Arc::clone(&peak);
                let gauge = Arc::clone(&gauge);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    publish_monotonic_peak(&peak, &gauge, value);
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        for thread in threads {
            thread.join().expect("peak publisher");
        }

        assert_eq!(peak.load(Ordering::Acquire), 1_024);
        assert_eq!(gauge.get(), 1_024);
        publish_monotonic_peak(&peak, &gauge, 2);
        assert_eq!(
            gauge.get(),
            1_024,
            "a late lower value must not erase the peak"
        );
    }
}
