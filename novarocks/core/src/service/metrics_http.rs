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
use std::thread::JoinHandle;
use std::time::Duration;

use axum::extract::Query;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};
use once_cell::sync::Lazy;
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, IntGaugeVec, TextEncoder,
    register_histogram, register_int_counter, register_int_gauge, register_int_gauge_vec,
};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;

/// Dedicated HTTP listener for process metrics.  Native execution continues to
/// expose `/metrics` on its gRPC/HTTP endpoint for compatibility, while
/// backend deployments use this listener at the configured HTTP port.
pub struct MetricsHttpServer {
    shutdown_tx: Option<watch::Sender<bool>>,
    join_handle: Option<JoinHandle<()>>,
}

impl MetricsHttpServer {
    /// Use this when the caller intentionally shares `/metrics` with a
    /// gRPC/HTTP listener on the same configured port.
    pub const fn shared_with_grpc() -> Self {
        Self {
            shutdown_tx: None,
            join_handle: None,
        }
    }

    pub fn start(host: &str, port: u16) -> Result<Self, String> {
        let bind_addr = parse_metrics_bind_addr(host, port)
            .map_err(|error| format!("parse metrics HTTP bind address failed: {error}"))?;
        let listener = TcpListener::bind(bind_addr).map_err(|error| {
            format!("bind metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            format!("configure metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let join_handle = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("failed to build metrics HTTP runtime: {error}");
                    return;
                }
            };
            runtime.block_on(async move {
                let listener = match TokioTcpListener::from_std(listener) {
                    Ok(listener) => listener,
                    Err(error) => {
                        eprintln!("failed to create metrics HTTP listener: {error}");
                        return;
                    }
                };
                let app = Router::new().route("/metrics", get(handle_metrics));
                if let Err(error) = axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        while !*shutdown_rx.borrow() {
                            if shutdown_rx.changed().await.is_err() {
                                break;
                            }
                        }
                    })
                    .await
                {
                    eprintln!("metrics HTTP server exited with error: {error}");
                }
            });
        });
        Ok(Self {
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        })
    }

    pub fn stop(mut self) -> Result<(), String> {
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

static FRAGMENT_SCHEDULED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "novarocks_fragment_scheduled_total",
        "Total number of plan fragment instances scheduled to backends."
    )
    .expect("register novarocks_fragment_scheduled_total")
});

static HEARTBEAT_RTT_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(HistogramOpts::new(
        "novarocks_heartbeat_rtt_seconds",
        "Backend heartbeat round-trip time in seconds."
    ))
    .expect("register novarocks_heartbeat_rtt_seconds")
});

static LIVE_BACKENDS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "novarocks_live_backends",
        "Number of live backends in the FE registry."
    )
    .expect("register novarocks_live_backends")
});

static BACKENDS_BY_STATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backends",
        "Number of backends by registry state.",
        &["state"]
    )
    .expect("register novarocks_backends")
});

static BACKEND_QUERY_LIFECYCLE_ENTRIES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backend_query_lifecycle_entries",
        "Number of backend query lifecycle entries by state.",
        &["state"]
    )
    .expect("register novarocks_backend_query_lifecycle_entries")
});

static BACKEND_QUERY_LIFECYCLE_REJECTIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backend_query_lifecycle_rejections",
        "Cumulative backend query lifecycle rejections by reason.",
        &["reason"]
    )
    .expect("register novarocks_backend_query_lifecycle_rejections")
});

static BACKEND_QUERY_LIFECYCLE_TERMINATIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backend_query_lifecycle_terminations",
        "Cumulative backend query lifecycle terminations by reason.",
        &["reason"]
    )
    .expect("register novarocks_backend_query_lifecycle_terminations")
});

static BACKEND_QUERY_LIFECYCLE_TERMINAL: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backend_query_lifecycle_terminal_total",
        "Cumulative backend query terminal lifecycle outcomes.",
        &["outcome"]
    )
    .expect("register novarocks_backend_query_lifecycle_terminal_total")
});

static BACKEND_QUERY_EXECUTION_RESOURCES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_backend_query_execution_resources",
        "Backend execution resources as reported by their owning component.",
        &["resource"]
    )
    .expect("register novarocks_backend_query_execution_resources")
});

static FRONTEND_QUERY_LIFECYCLE_ATTEMPTS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "novarocks_frontend_query_lifecycle_active_attempts",
        "Number of frontend-owned query lifecycle attempts."
    )
    .expect("register novarocks_frontend_query_lifecycle_active_attempts")
});

static FRONTEND_QUERY_LIFECYCLE_INIT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_frontend_query_lifecycle_init_total",
        "Cumulative frontend query initialization outcomes.",
        &["outcome"]
    )
    .expect("register novarocks_frontend_query_lifecycle_init_total")
});

static FRONTEND_QUERY_LIFECYCLE_CONTROL: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_frontend_query_lifecycle_control_total",
        "Cumulative frontend query control outcomes.",
        &["outcome"]
    )
    .expect("register novarocks_frontend_query_lifecycle_control_total")
});

static FRONTEND_QUERY_LIFECYCLE_LATENCY: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_frontend_query_lifecycle_latency_micros",
        "Cumulative frontend query lifecycle latency and sample counts.",
        &["phase", "measure"]
    )
    .expect("register novarocks_frontend_query_lifecycle_latency_micros")
});

static FRONTEND_CATALOG_PROJECTION_CATALOGS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "novarocks_frontend_catalog_projection_catalogs",
        "Number of catalog attachments currently projected into this Frontend runtime."
    )
    .expect("register novarocks_frontend_catalog_projection_catalogs")
});

static FRONTEND_CATALOG_PROJECTION_EVENTS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "novarocks_frontend_catalog_projection_events_total",
        "Cumulative Frontend catalog projection controller events.",
        &["outcome"]
    )
    .expect("register novarocks_frontend_catalog_projection_events_total")
});

pub(crate) fn observe_fragments_scheduled(count: usize) {
    Lazy::force(&FRAGMENT_SCHEDULED_TOTAL).inc_by(count as u64);
}

/// Record an FE-owned backend heartbeat observation without coupling the
/// heartbeat transport to Core's former gRPC client implementation.
pub fn observe_backend_heartbeat_rtt(duration: Duration) {
    Lazy::force(&HEARTBEAT_RTT_SECONDS).observe(duration.as_secs_f64());
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

/// Publishes already-counted backend registry states as neutral scalars.
///
/// Backend membership authority is owned outside this module (`ADR-0013`), so
/// the metrics surface deliberately names no membership type: it accepts the
/// counts and owns only the `novarocks_backends` label set they map onto. That
/// keeps the membership owner and this listener independently relocatable.
pub(crate) fn publish_backend_topology_metrics(
    registering: usize,
    live: usize,
    lost: usize,
    decommissioning: usize,
) {
    Lazy::force(&LIVE_BACKENDS).set(live as i64);
    for (state_name, count) in [
        ("registering", registering),
        ("live", live),
        ("lost", lost),
        ("decommissioning", decommissioning),
    ] {
        BACKENDS_BY_STATE
            .with_label_values(&[state_name])
            .set(count as i64);
    }
}

pub fn publish_backend_query_lifecycle_metrics(
    snapshot: crate::query_execution::lifecycle::metrics::BackendQueryLifecycleMetricsSnapshot,
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

pub fn publish_frontend_query_lifecycle_metrics(
    snapshot: crate::query_execution::lifecycle::metrics::FrontendQueryLifecycleMetricsSnapshot,
) {
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_ATTEMPTS).set(snapshot.active_attempts as i64);
    for (outcome, count) in [
        ("applied", snapshot.init_applied),
        ("already_applied", snapshot.init_idempotent),
        ("failed", snapshot.init_failed),
        ("uncertain_cleanup", snapshot.init_uncertain_cleanup),
        ("manifest_conflict", snapshot.manifest_conflicts),
    ] {
        FRONTEND_QUERY_LIFECYCLE_INIT
            .with_label_values(&[outcome])
            .set(count as i64);
    }
    for (outcome, count) in [
        ("control_ready", snapshot.control_ready),
        ("attach_failed", snapshot.attach_failed),
        ("heartbeat_timeout", snapshot.heartbeat_timeouts),
        ("coordinator_lost", snapshot.coordinator_lost),
        ("local_failure", snapshot.local_failures),
        ("backend_epoch_mismatch", snapshot.backend_epoch_mismatches),
        ("cleanup_failure", snapshot.cleanup_failures),
        (
            "terminal_locally_drained",
            snapshot.terminal_locally_drained,
        ),
        (
            "terminal_snapshot_accepted",
            snapshot.terminal_snapshots_accepted,
        ),
        (
            "terminal_snapshot_idempotent",
            snapshot.terminal_snapshots_idempotent,
        ),
        (
            "terminal_snapshot_conflict",
            snapshot.terminal_snapshot_conflicts,
        ),
        (
            "terminal_finalize_failure",
            snapshot.terminal_finalize_failures,
        ),
    ] {
        FRONTEND_QUERY_LIFECYCLE_CONTROL
            .with_label_values(&[outcome])
            .set(count as i64);
    }
    for (phase, total, samples) in [
        (
            "init",
            snapshot.init_latency_micros_total,
            snapshot.init_latency_samples,
        ),
        (
            "attach",
            snapshot.attach_latency_micros_total,
            snapshot.attach_latency_samples,
        ),
    ] {
        FRONTEND_QUERY_LIFECYCLE_LATENCY
            .with_label_values(&[phase, "total"])
            .set(total as i64);
        FRONTEND_QUERY_LIFECYCLE_LATENCY
            .with_label_values(&[phase, "samples"])
            .set(samples as i64);
    }
}

pub(crate) fn publish_catalog_projection_metrics(
    snapshot: crate::catalog_application::CatalogProjectionMetricsSnapshot,
) {
    Lazy::force(&FRONTEND_CATALOG_PROJECTION_CATALOGS).set(snapshot.projected_catalogs as i64);
    for (outcome, count) in [
        ("poll_success", snapshot.successful_polls),
        ("poll_failure", snapshot.failed_polls),
        ("resync", snapshot.resyncs),
        ("freshness_expiry", snapshot.freshness_expiries),
    ] {
        FRONTEND_CATALOG_PROJECTION_EVENTS
            .with_label_values(&[outcome])
            .set(count as i64);
    }
}

/// Shared metrics HTTP handler for role-owned native listeners.
pub async fn handle_metrics(Query(params): Query<HashMap<String, String>>) -> Response {
    if params
        .get("type")
        .is_some_and(|value| value.eq_ignore_ascii_case("json"))
    {
        return match render_metrics_json() {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
        };
    }

    match render_metrics() {
        Ok(body) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

/// Renders the process metrics in Prometheus text format for any listener
/// host. The caller owns HTTP route composition.
pub fn render_metrics() -> Result<String, String> {
    refresh_backend_gauges();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&prometheus::gather(), &mut buf)
        .map_err(|e| format!("encode prometheus metrics failed: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("prometheus metrics were not utf-8: {e}"))
}

/// Renders the process metrics in the existing JSON form for listener hosts.
pub fn render_metrics_json() -> Result<String, String> {
    refresh_backend_gauges();
    let mut rows = Vec::new();
    for family in prometheus::gather() {
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
    Lazy::force(&FRAGMENT_SCHEDULED_TOTAL);
    novarocks_execution::runtime::fragment::io::exchange_metrics::ensure_exchange_metrics_registered();
    Lazy::force(&HEARTBEAT_RTT_SECONDS);
    Lazy::force(&LIVE_BACKENDS);
    Lazy::force(&BACKENDS_BY_STATE);
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_ENTRIES);
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_REJECTIONS);
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_TERMINATIONS);
    Lazy::force(&BACKEND_QUERY_LIFECYCLE_TERMINAL);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_ATTEMPTS);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_INIT);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_CONTROL);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_LATENCY);
    Lazy::force(&FRONTEND_CATALOG_PROJECTION_CATALOGS);
    Lazy::force(&FRONTEND_CATALOG_PROJECTION_EVENTS);
}

#[cfg(test)]
mod tests {
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
    fn rendered_metrics_include_cluster_core_names() {
        observe_fragments_scheduled(1);
        novarocks_execution::runtime::fragment::io::exchange_metrics::observe_exchange_shuffle_bytes(7);
        observe_backend_heartbeat_rtt(Duration::from_millis(5));

        let body = render_metrics().expect("render metrics");
        assert!(body.contains("novarocks_fragment_scheduled_total"));
        assert!(body.contains("novarocks_exchange_shuffle_bytes_total"));
        assert!(body.contains("novarocks_heartbeat_rtt_seconds"));
        assert!(body.contains("novarocks_live_backends"));
        assert!(body.contains("novarocks_backends"));
    }

    #[test]
    fn backend_topology_gauges_preserve_the_last_nonzero_frontend_snapshot() {
        publish_backend_topology_metrics(1, 2, 3, 4);

        let first = render_metrics().expect("render first metrics snapshot");
        let second = render_metrics().expect("render second metrics snapshot");

        for body in [&first, &second] {
            assert!(body.contains("novarocks_live_backends 2"), "{body}");
            assert!(
                body.contains("novarocks_backends{state=\"registering\"} 1"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backends{state=\"live\"} 2"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backends{state=\"lost\"} 3"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backends{state=\"decommissioning\"} 4"),
                "{body}"
            );
        }
    }

    #[test]
    fn rendered_json_metrics_are_fe_metrics_compatible_array() {
        observe_fragments_scheduled(1);
        let body = render_metrics_json().expect("render json metrics");
        let value: serde_json::Value = serde_json::from_str(&body).expect("parse json metrics");
        let rows = value.as_array().expect("json metrics array");
        assert!(rows.iter().any(|row| {
            row.get("tags")
                .and_then(|tags| tags.get("metric"))
                .and_then(serde_json::Value::as_str)
                == Some("novarocks_fragment_scheduled_total")
        }));
    }

    #[test]
    fn frontend_query_lifecycle_metrics_publish_structured_snapshot() {
        publish_frontend_query_lifecycle_metrics(
            crate::query_execution::lifecycle::metrics::FrontendQueryLifecycleMetricsSnapshot {
                active_attempts: 2,
                init_applied: 3,
                init_idempotent: 4,
                init_failed: 5,
                init_uncertain_cleanup: 6,
                manifest_conflicts: 7,
                init_latency_micros_total: 8,
                init_latency_samples: 9,
                control_ready: 10,
                attach_failed: 11,
                attach_latency_micros_total: 12,
                attach_latency_samples: 13,
                heartbeat_timeouts: 14,
                coordinator_lost: 15,
                local_failures: 16,
                backend_epoch_mismatches: 17,
                cleanup_failures: 18,
                terminal_locally_drained: 19,
                terminal_snapshots_accepted: 20,
                terminal_snapshots_idempotent: 21,
                terminal_snapshot_conflicts: 22,
                terminal_finalize_failures: 23,
            },
        );

        let body = render_metrics().expect("render frontend query lifecycle metrics");
        assert!(
            body.contains("novarocks_frontend_query_lifecycle_active_attempts 2"),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_control_total{outcome=\"terminal_snapshot_accepted\"} 20"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_init_total{outcome=\"already_applied\"} 4"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_control_total{outcome=\"heartbeat_timeout\"} 14"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_latency_micros{measure=\"samples\",phase=\"attach\"} 13"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_control_total{outcome=\"local_failure\"} 16"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_control_total{outcome=\"cleanup_failure\"} 18"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_init_total{outcome=\"uncertain_cleanup\"} 6"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "novarocks_frontend_query_lifecycle_control_total{outcome=\"backend_epoch_mismatch\"} 17"
            ),
            "{body}"
        );
    }
}
