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

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;

use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};
use once_cell::sync::Lazy;
use prometheus::{Encoder, IntGaugeVec, Opts, Registry, TextEncoder};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;

/// Explicitly-owned Backend metric registry.  It is intentionally separate
/// from Prometheus' process-global registry so an all-in-one process cannot
/// leak a foreign role's metric families through the BE management endpoint.
pub(crate) struct BackendMetricsRegistry {
    registry: Registry,
}

impl BackendMetricsRegistry {
    pub(crate) fn new() -> Result<Self, String> {
        let registry = Registry::new();
        let collectors = [
            Box::new(Lazy::force(&BACKEND_QUERY_LIFECYCLE_ENTRIES).clone())
                as Box<dyn prometheus::core::Collector>,
            Box::new(Lazy::force(&BACKEND_QUERY_LIFECYCLE_REJECTIONS).clone()),
            Box::new(Lazy::force(&BACKEND_QUERY_LIFECYCLE_TERMINATIONS).clone()),
            Box::new(Lazy::force(&BACKEND_QUERY_LIFECYCLE_TERMINAL).clone()),
            Box::new(Lazy::force(&BACKEND_QUERY_EXECUTION_RESOURCES).clone()),
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

/// Dedicated HTTP listener for Backend management metrics.
pub struct MetricsHttpServer {
    shutdown_tx: Option<watch::Sender<bool>>,
    failure_rx: mpsc::Receiver<String>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Arc<AtomicBool>,
}

impl MetricsHttpServer {
    pub fn start(
        host: &str,
        port: u16,
        metrics: Arc<BackendMetricsRegistry>,
    ) -> Result<Self, String> {
        let bind_addr = parse_metrics_bind_addr(host, port)
            .map_err(|error| format!("parse metrics HTTP bind address failed: {error}"))?;
        let listener = TcpListener::bind(bind_addr).map_err(|error| {
            format!("bind metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            format!("configure metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let thread_stop_requested = Arc::clone(&stop_requested);
        let join_handle = std::thread::Builder::new()
            .name("backend-management-http".to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| {
                            format!("build backend management HTTP runtime: {error}")
                        })?;
                    runtime.block_on(async move {
                        let listener = TokioTcpListener::from_std(listener).map_err(|error| {
                            format!("create Tokio backend management HTTP listener: {error}")
                        })?;
                        let app = Router::new()
                            .route("/metrics", get(handle_metrics))
                            .with_state(metrics);
                        axum::serve(listener, app)
                            .with_graceful_shutdown(async move {
                                while !*shutdown_rx.borrow() {
                                    if shutdown_rx.changed().await.is_err() {
                                        break;
                                    }
                                }
                            })
                            .await
                            .map_err(|error| {
                                format!("backend management HTTP serve future failed: {error}")
                            })
                    })
                }));
                if thread_stop_requested.load(Ordering::Acquire) {
                    return;
                }
                let error = match outcome {
                    Ok(Ok(())) => "backend management HTTP server exited unexpectedly".to_string(),
                    Ok(Err(error)) => error,
                    Err(payload) => payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| {
                            payload
                                .downcast_ref::<&str>()
                                .map(|value| (*value).to_string())
                        })
                        .unwrap_or_else(|| "backend management HTTP server panicked".to_string()),
                };
                let _ = failure_tx.send(error);
            })
            .map_err(|error| format!("spawn backend management HTTP server: {error}"))?;
        Ok(Self {
            shutdown_tx: Some(shutdown_tx),
            failure_rx,
            join_handle: Some(join_handle),
            stop_requested,
        })
    }

    pub fn poll_failure(&mut self) -> Result<Option<String>, String> {
        match self.failure_rx.try_recv() {
            Ok(error) => Ok(Some(error)),
            Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn stop(mut self) -> Result<(), String> {
        self.stop_requested.store(true, Ordering::Release);
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .map_err(|_| "metrics HTTP server thread panicked".to_string())?;
        }
        Ok(())
    }
}

static BACKEND_QUERY_LIFECYCLE_ENTRIES: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_query_lifecycle_entries",
            "Number of backend query lifecycle entries by state.",
        ),
        &["state"],
    )
    .expect("construct novarocks_backend_query_lifecycle_entries")
});

static BACKEND_QUERY_LIFECYCLE_REJECTIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_query_lifecycle_rejections",
            "Cumulative backend query lifecycle rejections by reason.",
        ),
        &["reason"],
    )
    .expect("construct novarocks_backend_query_lifecycle_rejections")
});

static BACKEND_QUERY_LIFECYCLE_TERMINATIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_query_lifecycle_terminations",
            "Cumulative backend query lifecycle terminations by reason.",
        ),
        &["reason"],
    )
    .expect("construct novarocks_backend_query_lifecycle_terminations")
});

static BACKEND_QUERY_LIFECYCLE_TERMINAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_query_lifecycle_terminal_total",
            "Cumulative backend query terminal lifecycle outcomes.",
        ),
        &["outcome"],
    )
    .expect("construct novarocks_backend_query_lifecycle_terminal_total")
});

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
pub(crate) fn record_connector_write_writer_open(outcome: &'static str) {
    BACKEND_CONNECTOR_WRITE_WRITER_OPENS
        .with_label_values(&[outcome])
        .inc();
}

/// One connector writer finished: it accepted `rows` rows and produced
/// `fragments` commit fragments.
pub(crate) fn record_connector_write_writer_finished(rows: u64, fragments: u64) {
    BACKEND_CONNECTOR_WRITE_WRITER_TOTALS
        .with_label_values(&["rows"])
        .inc_by(rows);
    BACKEND_CONNECTOR_WRITE_WRITER_TOTALS
        .with_label_values(&["commit_fragments"])
        .inc_by(fragments);
}

pub(crate) fn record_connector_write_writer_abort(outcome: &'static str) {
    BACKEND_CONNECTOR_WRITE_WRITER_ABORTS
        .with_label_values(&[outcome])
        .inc();
}

#[cfg(debug_assertions)]
pub(crate) fn record_connector_write_debug_fault(kind: &'static str) {
    BACKEND_CONNECTOR_WRITE_DEBUG_FAULTS
        .with_label_values(&[kind])
        .inc();
}

pub(crate) fn record_fragment_result_terminal(terminal: &'static str) {
    BACKEND_FRAGMENT_RESULT_TERMINALS
        .with_label_values(&[terminal])
        .inc();
}

/// Publish the prepared write set a root aggregation has accepted so far. The
/// gauge keeps the process-wide high-water mark, so a later, smaller write
/// never erases the peak an operator needs in order to size the budgets.
pub(crate) fn publish_connector_write_root_prepared_set_peak(bytes: u64, entries: u64) {
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

pub(crate) fn record_backend_native_authentication_failure() {
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

pub(crate) fn record_backend_native_tls_handshake_failure() {
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

fn parse_metrics_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let bare = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        host
    };
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let formatted = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    formatted
        .parse::<SocketAddr>()
        .map_err(|error| format!("parse metrics bind addr '{formatted}' failed: {error}"))
}

pub fn publish_backend_query_lifecycle_metrics(
    snapshot: crate::metrics::query_lifecycle::BackendQueryLifecycleMetricsSnapshot,
    termination_reasons: [u64; 6],
) {
    for (state_name, count) in [
        ("initializing", snapshot.initializing),
        ("initialized", snapshot.initialized),
        ("control_attached", snapshot.control_attached),
        ("terminating", snapshot.terminating),
        ("tombstone", snapshot.tombstones),
    ] {
        BACKEND_QUERY_LIFECYCLE_ENTRIES
            .with_label_values(&[state_name])
            .set(count as i64);
    }
    for (reason, count) in [
        ("admission", snapshot.admission_rejected),
        ("init_conflict", snapshot.init_conflicts),
        ("heartbeat_timeout", snapshot.heartbeat_timeouts),
        (
            "terminal_fallback_rejected",
            snapshot.terminal_fallback_rejected,
        ),
    ] {
        BACKEND_QUERY_LIFECYCLE_REJECTIONS
            .with_label_values(&[reason])
            .set(count as i64);
    }
    for (reason, count) in [
        ("coordinator_abort", termination_reasons[0]),
        ("coordinator_finalize", termination_reasons[1]),
        ("coordinator_stream_lost", termination_reasons[2]),
        ("coordinator_heartbeat_timeout", termination_reasons[3]),
        ("local_failure", termination_reasons[4]),
        ("pre_start_timeout", termination_reasons[5]),
    ] {
        BACKEND_QUERY_LIFECYCLE_TERMINATIONS
            .with_label_values(&[reason])
            .set(count as i64);
    }
    for (outcome, count) in [
        ("terminal_fact", snapshot.terminal_facts),
        ("terminal_local_drained", snapshot.terminal_locally_drained),
        ("terminal_record_frozen", snapshot.terminal_records_frozen),
        ("terminal_acknowledged", snapshot.terminal_acknowledged),
        (
            "terminal_retention_expired",
            snapshot.terminal_retention_expired,
        ),
        (
            "terminal_fallback_accepted",
            snapshot.terminal_fallback_accepted,
        ),
        ("terminal_retained", snapshot.terminal_retained as u64),
        (
            "terminal_retained_bytes",
            snapshot.terminal_retained_bytes as u64,
        ),
    ] {
        BACKEND_QUERY_LIFECYCLE_TERMINAL
            .with_label_values(&[outcome])
            .set(count as i64);
    }
}

/// Publish a scalar snapshot after its owner has released its own lock. The
/// metrics layer deliberately holds no execution resource references.
pub fn publish_backend_query_execution_resource(resource: &'static str, value: usize) {
    BACKEND_QUERY_EXECUTION_RESOURCES
        .with_label_values(&[resource])
        .set(value as i64);
}

pub fn publish_backend_query_lifecycle_terminal_limits(capacity: usize, max_bytes: usize) {
    BACKEND_QUERY_LIFECYCLE_TERMINAL
        .with_label_values(&["terminal_retained_capacity"])
        .set(capacity as i64);
    BACKEND_QUERY_LIFECYCLE_TERMINAL
        .with_label_values(&["terminal_max_retained_bytes"])
        .set(max_bytes as i64);
}

async fn handle_metrics(
    State(metrics): State<Arc<BackendMetricsRegistry>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if params
        .get("type")
        .is_some_and(|value| value.eq_ignore_ascii_case("json"))
    {
        return match render_metrics_json(&metrics) {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
        };
    }

    match render_metrics(&metrics) {
        Ok(body) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
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

fn refresh_backend_gauges() {
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_ENTRIES);
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_REJECTIONS);
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_TERMINATIONS);
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_TERMINAL);
    Lazy::force(&BACKEND_QUERY_EXECUTION_RESOURCES);
    ensure_backend_metric_label_families();
}

/// Make the documented BE metric families observable before their first event
/// without resetting values already published by their application owner.
fn ensure_backend_metric_label_families() {
    for state in [
        "initializing",
        "initialized",
        "control_attached",
        "terminating",
        "tombstone",
    ] {
        let _ = BACKEND_QUERY_LIFECYCLE_ENTRIES.get_metric_with_label_values(&[state]);
    }
    for reason in [
        "admission",
        "init_conflict",
        "heartbeat_timeout",
        "terminal_fallback_rejected",
    ] {
        let _ = BACKEND_QUERY_LIFECYCLE_REJECTIONS.get_metric_with_label_values(&[reason]);
    }
    for reason in [
        "coordinator_abort",
        "coordinator_finalize",
        "coordinator_stream_lost",
        "coordinator_heartbeat_timeout",
        "local_failure",
        "pre_start_timeout",
    ] {
        let _ = BACKEND_QUERY_LIFECYCLE_TERMINATIONS.get_metric_with_label_values(&[reason]);
    }
    for outcome in [
        "terminal_fact",
        "terminal_local_drained",
        "terminal_record_frozen",
        "terminal_acknowledged",
        "terminal_retention_expired",
        "terminal_fallback_accepted",
        "terminal_retained",
        "terminal_retained_bytes",
        "terminal_retained_capacity",
        "terminal_max_retained_bytes",
    ] {
        let _ = BACKEND_QUERY_LIFECYCLE_TERMINAL.get_metric_with_label_values(&[outcome]);
    }
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
    use std::sync::Barrier;

    use prometheus::{IntGauge, Opts, Registry};

    use super::*;

    #[test]
    fn metrics_bind_addr_accepts_ipv4_and_ipv6_literals() {
        assert_eq!(
            parse_metrics_bind_addr("127.0.0.1", 9070).expect("parse IPv4"),
            "127.0.0.1:9070"
                .parse::<SocketAddr>()
                .expect("IPv4 address")
        );
        assert_eq!(
            parse_metrics_bind_addr("::1", 9070).expect("parse bare IPv6"),
            "[::1]:9070".parse::<SocketAddr>().expect("IPv6 address")
        );
        assert_eq!(
            parse_metrics_bind_addr("[::]", 9070).expect("parse bracketed IPv6"),
            "[::]:9070".parse::<SocketAddr>().expect("IPv6 wildcard")
        );
    }

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
        assert!(rendered.contains("novarocks_backend_query_lifecycle_entries"));
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
