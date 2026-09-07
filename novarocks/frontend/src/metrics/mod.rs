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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use once_cell::sync::Lazy;
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};

use crate::workload_lifecycle::{
    FrontendCatalogSourceMode, FrontendServingSnapshot, FrontendServingState,
};

pub(crate) mod dml_publication;
mod http;
mod management;
pub mod process_query_counters;
pub(crate) use http::{LateBoundQueryLifecycleConvergenceReader, MetricsHttpServer};
pub use process_query_counters::FrontendProcessQueryCountersSnapshot;

static FRAGMENT_SCHEDULED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(Opts::new(
        "novarocks_fragment_scheduled_total",
        "Total number of plan fragment instances scheduled to backends.",
    ))
    .expect("register novarocks_fragment_scheduled_total")
});

static HEARTBEAT_RTT_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "novarocks_heartbeat_rtt_seconds",
        "Backend heartbeat round-trip time in seconds.",
    ))
    .expect("register novarocks_heartbeat_rtt_seconds")
});

static BACKEND_REGISTRY_ENTRIES: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_backend_registry_entries",
        "Number of BE process descriptors retained by the FE runtime registry.",
    ))
    .expect("register novarocks_backend_registry_entries")
});

static BACKEND_ANNOUNCE_LEASE_VALID: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_backend_announce_lease_valid",
        "Number of BE entries with a valid self-registration lease.",
    ))
    .expect("register novarocks_backend_announce_lease_valid")
});

static BACKEND_IDENTITY_VERIFIED: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_backend_identity_verified",
        "Number of BE entries whose exact descriptor was verified by FE pull heartbeat.",
    ))
    .expect("register novarocks_backend_identity_verified")
});

static BACKEND_REPORTED_STATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_reported_state",
            "Number of BE entries by self-reported process state.",
        ),
        &["state"],
    )
    .expect("register novarocks_backend_reported_state")
});

static BACKEND_COMPATIBILITY: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_compatibility",
            "Number of BE entries by FE compatibility result.",
        ),
        &["state"],
    )
    .expect("register novarocks_backend_compatibility")
});

static BACKEND_ENDPOINT_OWNERSHIP: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_endpoint_ownership",
            "Number of BE entries by verified endpoint ownership.",
        ),
        &["state"],
    )
    .expect("register novarocks_backend_endpoint_ownership")
});

static BACKEND_ELIGIBLE: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_backend_eligible",
        "Number of BE entries eligible for new query admission.",
    ))
    .expect("register novarocks_backend_eligible")
});

static BACKEND_TOPOLOGY_REVISION: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_backend_topology_revision",
        "Latest FE topology revision for the eligible BE set.",
    ))
    .expect("register novarocks_backend_topology_revision")
});

static BACKEND_ANNOUNCE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_backend_announce_total",
            "Total BE self-registration announce outcomes observed by FE.",
        ),
        &["outcome"],
    )
    .expect("register novarocks_backend_announce_total")
});

static BACKEND_HEARTBEAT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_backend_heartbeat_total",
            "Total FE-pull BE heartbeat outcomes.",
        ),
        &["outcome"],
    )
    .expect("register novarocks_backend_heartbeat_total")
});

static PRE_READY_REPLAN_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_pre_ready_replan_total",
            "Total pre-ControlReady whole-round replans by typed topology reason.",
        ),
        &["reason"],
    )
    .expect("register novarocks_pre_ready_replan_total")
});

static PRE_READY_EFFECT_GATE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_pre_ready_effect_gate_total",
            "Total pre-ControlReady effect-gate decisions.",
        ),
        &["outcome"],
    )
    .expect("register novarocks_pre_ready_effect_gate_total")
});

static WAITING_FOR_BACKEND_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "novarocks_waiting_for_backend_seconds",
        "Time spent waiting for a newer eligible backend topology revision.",
    ))
    .expect("register novarocks_waiting_for_backend_seconds")
});

static PRE_READY_REPLAN_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "novarocks_pre_ready_replan_seconds",
        "Time spent rebuilding one replacement distributed round.",
    ))
    .expect("register novarocks_pre_ready_replan_seconds")
});
static FRONTEND_QUERY_LIFECYCLE_ATTEMPTS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_frontend_query_lifecycle_active_attempts",
        "Number of frontend-owned query lifecycle attempts.",
    ))
    .expect("register novarocks_frontend_query_lifecycle_active_attempts")
});

static FRONTEND_QUERY_LIFECYCLE_INIT: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_query_lifecycle_init_total",
            "Cumulative frontend query initialization outcomes.",
        ),
        &["outcome"],
    )
    .expect("register novarocks_frontend_query_lifecycle_init_total")
});

static FRONTEND_QUERY_LIFECYCLE_CONTROL: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_query_lifecycle_control_total",
            "Cumulative frontend query control outcomes.",
        ),
        &["outcome"],
    )
    .expect("register novarocks_frontend_query_lifecycle_control_total")
});

static FRONTEND_QUERY_LIFECYCLE_LATENCY: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_query_lifecycle_latency_micros",
            "Cumulative frontend query lifecycle latency and sample counts.",
        ),
        &["phase", "measure"],
    )
    .expect("register novarocks_frontend_query_lifecycle_latency_micros")
});

/// Bounded, role-local Native transport rejections. The label names the FE
/// listener family and never contains a caller, endpoint, token, or error.
static NATIVE_TRUST_TRANSPORT_REJECTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_native_trust_transport_rejections_total",
            "Native listener connections rejected before Frontend RPC dispatch.",
        ),
        &["listener"],
    )
    .expect("register novarocks_native_trust_transport_rejections_total")
});

static FRONTEND_SERVING_STATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_serving_state",
            "Frontend serving lifecycle state as a one-hot gauge.",
        ),
        &["state"],
    )
    .expect("register novarocks_frontend_serving_state")
});

static FRONTEND_BASE_READY: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_frontend_base_ready",
        "Whether this Frontend currently admits base workloads.",
    ))
    .expect("register novarocks_frontend_base_ready")
});

static FRONTEND_ISLAND_READY: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_frontend_island_ready",
        "Whether this Frontend is base-ready and has at least one compatible eligible Backend.",
    ))
    .expect("register novarocks_frontend_island_ready")
});

static FRONTEND_COMPATIBLE_ELIGIBLE_BACKENDS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_frontend_compatible_eligible_backends",
        "Number of compatible eligible Backends in this Frontend compatibility island.",
    ))
    .expect("register novarocks_frontend_compatible_eligible_backends")
});

static FRONTEND_CATALOG_SOURCE_MODE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_catalog_source_mode",
            "Selected catalog desired-state source mode as a one-hot gauge.",
        ),
        &["mode"],
    )
    .expect("register novarocks_frontend_catalog_source_mode")
});

static FRONTEND_CATALOGS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_catalogs",
            "Catalog bootstrap aggregate counts by sanitized state.",
        ),
        &["state"],
    )
    .expect("register novarocks_frontend_catalogs")
});

static FRONTEND_WORKLOAD_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_workload_active",
            "Active admitted Frontend workloads by kind.",
        ),
        &["kind"],
    )
    .expect("register novarocks_frontend_workload_active")
});

static FRONTEND_WORKLOAD_ADMISSION_REJECTED: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_frontend_workload_admission_rejected_total",
            "Total rejected Frontend workload admissions by kind.",
        ),
        &["kind"],
    )
    .expect("register novarocks_frontend_workload_admission_rejected_total")
});

static FRONTEND_WORKLOAD_COMPLETED_DURING_DRAIN: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_frontend_workload_completed_during_drain_total",
            "Total admitted Frontend workloads that completed while draining.",
        ),
        &["kind"],
    )
    .expect("register novarocks_frontend_workload_completed_during_drain_total")
});

static FRONTEND_WORKLOAD_DRAIN_DEADLINE_CANCELLED: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_frontend_workload_drain_deadline_cancelled_total",
            "Total admitted Frontend workloads cancelled at the drain deadline.",
        ),
        &["kind"],
    )
    .expect("register novarocks_frontend_workload_drain_deadline_cancelled_total")
});

static FRONTEND_DRAIN_STARTED_TIME_SECONDS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_frontend_drain_started_time_seconds",
        "Unix timestamp at which Frontend drain began, or zero before drain.",
    ))
    .expect("register novarocks_frontend_drain_started_time_seconds")
});

static FRONTEND_DRAIN_DEADLINE_TIME_SECONDS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_frontend_drain_deadline_time_seconds",
        "Unix timestamp at which Frontend drain reaches its deadline, or zero before drain.",
    ))
    .expect("register novarocks_frontend_drain_deadline_time_seconds")
});

static FRONTEND_DRAIN_ELAPSED_SECONDS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_frontend_drain_elapsed_seconds",
        "Elapsed time spent draining this Frontend, or zero before drain.",
    ))
    .expect("register novarocks_frontend_drain_elapsed_seconds")
});

/// Publishing uses reset-plus-current-total for process-local lifecycle
/// counters. Serialize that sequence so a concurrent observation cannot turn
/// a cumulative total into an accidental double-count.
static FRONTEND_SERVING_METRICS_PUBLISH_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Explicit collector registry owned by one Frontend management host.
///
/// This keeps FE's metrics surface role-local even when a process supervises
/// both Frontend and Backend application hosts.
pub(crate) struct FrontendMetricsRegistry {
    registry: Registry,
}

impl FrontendMetricsRegistry {
    pub(crate) fn new() -> Result<Arc<Self>, String> {
        refresh_frontend_gauges();
        let registry = Registry::new();
        for collector in [
            Box::new(FRAGMENT_SCHEDULED_TOTAL.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(HEARTBEAT_RTT_SECONDS.clone()),
            Box::new(BACKEND_REGISTRY_ENTRIES.clone()),
            Box::new(BACKEND_ANNOUNCE_LEASE_VALID.clone()),
            Box::new(BACKEND_IDENTITY_VERIFIED.clone()),
            Box::new(BACKEND_REPORTED_STATE.clone()),
            Box::new(BACKEND_COMPATIBILITY.clone()),
            Box::new(BACKEND_ENDPOINT_OWNERSHIP.clone()),
            Box::new(BACKEND_ELIGIBLE.clone()),
            Box::new(BACKEND_TOPOLOGY_REVISION.clone()),
            Box::new(BACKEND_ANNOUNCE_TOTAL.clone()),
            Box::new(BACKEND_HEARTBEAT_TOTAL.clone()),
            Box::new(PRE_READY_REPLAN_TOTAL.clone()),
            Box::new(PRE_READY_EFFECT_GATE_TOTAL.clone()),
            Box::new(WAITING_FOR_BACKEND_SECONDS.clone()),
            Box::new(PRE_READY_REPLAN_SECONDS.clone()),
            Box::new(FRONTEND_QUERY_LIFECYCLE_ATTEMPTS.clone()),
            Box::new(FRONTEND_QUERY_LIFECYCLE_INIT.clone()),
            Box::new(FRONTEND_QUERY_LIFECYCLE_CONTROL.clone()),
            Box::new(FRONTEND_QUERY_LIFECYCLE_LATENCY.clone()),
            Box::new(NATIVE_TRUST_TRANSPORT_REJECTIONS.clone()),
            Box::new(FRONTEND_SERVING_STATE.clone()),
            Box::new(FRONTEND_BASE_READY.clone()),
            Box::new(FRONTEND_ISLAND_READY.clone()),
            Box::new(FRONTEND_COMPATIBLE_ELIGIBLE_BACKENDS.clone()),
            Box::new(FRONTEND_CATALOG_SOURCE_MODE.clone()),
            Box::new(FRONTEND_CATALOGS.clone()),
            Box::new(FRONTEND_WORKLOAD_ACTIVE.clone()),
            Box::new(FRONTEND_WORKLOAD_ADMISSION_REJECTED.clone()),
            Box::new(FRONTEND_WORKLOAD_COMPLETED_DURING_DRAIN.clone()),
            Box::new(FRONTEND_WORKLOAD_DRAIN_DEADLINE_CANCELLED.clone()),
            Box::new(FRONTEND_DRAIN_STARTED_TIME_SECONDS.clone()),
            Box::new(FRONTEND_DRAIN_DEADLINE_TIME_SECONDS.clone()),
            Box::new(FRONTEND_DRAIN_ELAPSED_SECONDS.clone()),
        ] {
            registry
                .register(collector)
                .map_err(|error| format!("register frontend metric collector failed: {error}"))?;
        }
        crate::catalog_projection_metrics::register_collectors(&registry)?;
        dml_publication::register_collectors(&registry)?;
        crate::native::task_transport::register_metric_collectors(&registry)?;
        Ok(Arc::new(Self { registry }))
    }

    fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }
}

pub(crate) fn observe_fragments_scheduled(count: usize) {
    Lazy::force(&FRAGMENT_SCHEDULED_TOTAL).inc_by(count as u64);
}

/// Record an FE-owned backend heartbeat observation without coupling the
/// heartbeat transport to Core's former gRPC client implementation.
pub fn observe_backend_heartbeat_rtt(duration: Duration) {
    Lazy::force(&HEARTBEAT_RTT_SECONDS).observe(duration.as_secs_f64());
}

pub(crate) fn observe_native_trust_transport_rejection(listener: &'static str) {
    NATIVE_TRUST_TRANSPORT_REJECTIONS
        .with_label_values(&[listener])
        .inc();
}

/// Publishes the low-cardinality management snapshot into the explicitly
/// registered Frontend metric family. No catalog name, property, credential,
/// connection, or attempt identity is a metric label.
pub(crate) fn publish_frontend_serving_metrics(snapshot: FrontendServingSnapshot) {
    let _publish_guard = FRONTEND_SERVING_METRICS_PUBLISH_LOCK
        .lock()
        .expect("frontend serving metrics publish lock poisoned");
    for state in [
        FrontendServingState::Starting,
        FrontendServingState::Ready,
        FrontendServingState::Draining,
        FrontendServingState::Stopping,
    ] {
        FRONTEND_SERVING_STATE
            .with_label_values(&[state.as_metric_label()])
            .set((snapshot.serving_state == state) as i64);
    }
    FRONTEND_BASE_READY.set(snapshot.base_ready() as i64);
    for mode in [
        FrontendCatalogSourceMode::StaticFile,
        FrontendCatalogSourceMode::DynamicStateStore,
        FrontendCatalogSourceMode::ManagedController,
    ] {
        FRONTEND_CATALOG_SOURCE_MODE
            .with_label_values(&[mode.as_metric_label()])
            .set((snapshot.catalog.source_mode == Some(mode)) as i64);
    }
    for (state, value) in [
        ("desired", snapshot.catalog.counts.desired),
        ("ready", snapshot.catalog.counts.ready),
        ("unavailable", snapshot.catalog.counts.unavailable),
    ] {
        FRONTEND_CATALOGS
            .with_label_values(&[state])
            .set(value as i64);
    }
    for (kind, active, completed, cancelled) in [
        (
            "statement",
            snapshot.workload.active.statement,
            snapshot.workload.completed_during_drain.statement,
            snapshot.workload.deadline_cancelled.statement,
        ),
        (
            "background",
            snapshot.workload.active.background,
            snapshot.workload.completed_during_drain.background,
            snapshot.workload.deadline_cancelled.background,
        ),
    ] {
        FRONTEND_WORKLOAD_ACTIVE
            .with_label_values(&[kind])
            .set(active as i64);
        let completed_metric = FRONTEND_WORKLOAD_COMPLETED_DURING_DRAIN.with_label_values(&[kind]);
        completed_metric.reset();
        completed_metric.inc_by(completed);
        let cancelled_metric =
            FRONTEND_WORKLOAD_DRAIN_DEADLINE_CANCELLED.with_label_values(&[kind]);
        cancelled_metric.reset();
        cancelled_metric.inc_by(cancelled);
    }
    for (kind, rejected) in [
        ("session", snapshot.workload.rejected_admissions.session),
        ("statement", snapshot.workload.rejected_admissions.statement),
        (
            "background",
            snapshot.workload.rejected_admissions.background,
        ),
    ] {
        let rejected_metric = FRONTEND_WORKLOAD_ADMISSION_REJECTED.with_label_values(&[kind]);
        rejected_metric.reset();
        rejected_metric.inc_by(rejected);
    }
    FRONTEND_DRAIN_STARTED_TIME_SECONDS.set(
        snapshot
            .drain
            .started_at_unix_ms
            .map_or(0, |millis| (millis / 1_000) as i64),
    );
    FRONTEND_DRAIN_DEADLINE_TIME_SECONDS.set(
        snapshot
            .drain
            .deadline_unix_ms
            .map_or(0, |millis| (millis / 1_000) as i64),
    );
    FRONTEND_DRAIN_ELAPSED_SECONDS.set((snapshot.drain.elapsed_ms / 1_000) as i64);
}

/// Publishes the management-composed island readiness result. The scalar
/// inputs deliberately contain no endpoint, build identity, or compatibility
/// ID, so this observability surface cannot become a second topology owner.
pub(crate) fn publish_frontend_island_metrics(ready: bool, compatible_eligible_backends: usize) {
    FRONTEND_ISLAND_READY.set(ready as i64);
    FRONTEND_COMPATIBLE_ELIGIBLE_BACKENDS.set(compatible_eligible_backends as i64);
}

pub(crate) fn record_backend_announce(outcome: &'static str) {
    BACKEND_ANNOUNCE_TOTAL.with_label_values(&[outcome]).inc();
}

pub(crate) fn record_backend_heartbeat(outcome: &'static str) {
    BACKEND_HEARTBEAT_TOTAL.with_label_values(&[outcome]).inc();
}

pub(crate) fn record_pre_ready_replan(reason: &'static str) {
    PRE_READY_REPLAN_TOTAL.with_label_values(&[reason]).inc();
}

pub(crate) fn record_pre_ready_effect_gate(outcome: &'static str) {
    PRE_READY_EFFECT_GATE_TOTAL
        .with_label_values(&[outcome])
        .inc();
}

pub(crate) fn observe_waiting_for_backend(duration: Duration) {
    WAITING_FOR_BACKEND_SECONDS.observe(duration.as_secs_f64());
}

pub(crate) fn observe_pre_ready_replan(duration: Duration) {
    PRE_READY_REPLAN_SECONDS.observe(duration.as_secs_f64());
}
/// Publishes already-counted backend registry facts as neutral scalars.
///
/// The runtime registry remains the membership owner. This module owns only
/// the stable metric names and label sets, so observability does not become a
/// second membership authority.
pub(crate) fn publish_backend_topology_metrics(
    entries: usize,
    announce_lease_valid: usize,
    identity_verified: usize,
    reported_running: usize,
    reported_draining: usize,
    compatibility_compatible: usize,
    compatibility_other_island: usize,
    compatibility_unknown_or_invalid: usize,
    endpoint_owned: usize,
    endpoint_unowned: usize,
    eligible: usize,
    revision: u64,
) {
    Lazy::force(&BACKEND_REGISTRY_ENTRIES).set(entries as i64);
    Lazy::force(&BACKEND_ANNOUNCE_LEASE_VALID).set(announce_lease_valid as i64);
    Lazy::force(&BACKEND_IDENTITY_VERIFIED).set(identity_verified as i64);
    Lazy::force(&BACKEND_ELIGIBLE).set(eligible as i64);
    Lazy::force(&BACKEND_TOPOLOGY_REVISION).set(revision as i64);
    for (metric, states) in [
        (
            &*BACKEND_REPORTED_STATE,
            [
                ("running", reported_running),
                ("draining", reported_draining),
                ("unspecified", 0),
            ],
        ),
        (
            &*BACKEND_COMPATIBILITY,
            [
                ("compatible", compatibility_compatible),
                ("other_island", compatibility_other_island),
                ("unknown_or_invalid", compatibility_unknown_or_invalid),
            ],
        ),
        (
            &*BACKEND_ENDPOINT_OWNERSHIP,
            [
                ("owned", endpoint_owned),
                ("unowned", endpoint_unowned),
                ("unknown", 0),
            ],
        ),
    ] {
        for (state_name, count) in states {
            metric.with_label_values(&[state_name]).set(count as i64);
        }
    }
}
pub fn publish_frontend_process_query_counters(snapshot: FrontendProcessQueryCountersSnapshot) {
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

/// Renders only the collectors explicitly registered by the Frontend host.
pub(crate) fn render_metrics(registry: &FrontendMetricsRegistry) -> Result<String, String> {
    refresh_frontend_gauges();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&registry.gather(), &mut buf)
        .map_err(|e| format!("encode prometheus metrics failed: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("prometheus metrics were not utf-8: {e}"))
}

/// Renders the Frontend registry in the existing JSON form.
pub(crate) fn render_metrics_json(registry: &FrontendMetricsRegistry) -> Result<String, String> {
    refresh_frontend_gauges();
    let mut rows = Vec::new();
    for family in registry.gather() {
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
fn refresh_frontend_gauges() {
    Lazy::force(&FRAGMENT_SCHEDULED_TOTAL);
    Lazy::force(&HEARTBEAT_RTT_SECONDS);
    Lazy::force(&BACKEND_REGISTRY_ENTRIES);
    Lazy::force(&BACKEND_ANNOUNCE_LEASE_VALID);
    Lazy::force(&BACKEND_IDENTITY_VERIFIED);
    Lazy::force(&BACKEND_REPORTED_STATE);
    Lazy::force(&BACKEND_COMPATIBILITY);
    Lazy::force(&BACKEND_ENDPOINT_OWNERSHIP);
    Lazy::force(&BACKEND_ELIGIBLE);
    Lazy::force(&BACKEND_TOPOLOGY_REVISION);
    Lazy::force(&BACKEND_ANNOUNCE_TOTAL);
    Lazy::force(&BACKEND_HEARTBEAT_TOTAL);
    Lazy::force(&PRE_READY_REPLAN_TOTAL);
    Lazy::force(&PRE_READY_EFFECT_GATE_TOTAL);
    Lazy::force(&WAITING_FOR_BACKEND_SECONDS);
    Lazy::force(&PRE_READY_REPLAN_SECONDS);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_ATTEMPTS);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_INIT);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_CONTROL);
    Lazy::force(&FRONTEND_QUERY_LIFECYCLE_LATENCY);
    Lazy::force(&FRONTEND_ISLAND_READY);
    Lazy::force(&FRONTEND_COMPATIBLE_ELIGIBLE_BACKENDS);
    dml_publication::ensure_label_families();
    ensure_frontend_metric_label_families();
}

/// Make the documented FE metric families observable before their first event
/// without resetting values already published by their application owner.
fn ensure_frontend_metric_label_families() {
    for state in ["running", "draining", "unspecified"] {
        let _ = BACKEND_REPORTED_STATE.get_metric_with_label_values(&[state]);
    }
    for state in ["compatible", "other_island", "unknown_or_invalid"] {
        let _ = BACKEND_COMPATIBILITY.get_metric_with_label_values(&[state]);
    }
    for state in ["owned", "unowned", "unknown"] {
        let _ = BACKEND_ENDPOINT_OWNERSHIP.get_metric_with_label_values(&[state]);
    }
    for outcome in ["accepted", "rejected"] {
        let _ = BACKEND_ANNOUNCE_TOTAL.get_metric_with_label_values(&[outcome]);
    }
    for outcome in ["verified", "identity_mismatch", "unknown_process", "failed"] {
        let _ = BACKEND_HEARTBEAT_TOTAL.get_metric_with_label_values(&[outcome]);
    }
    for reason in [
        "backend_draining",
        "backend_process_mismatch",
        "backend_not_eligible",
    ] {
        let _ = PRE_READY_REPLAN_TOTAL.get_metric_with_label_values(&[reason]);
    }
    for outcome in ["permitted", "rejected"] {
        let _ = PRE_READY_EFFECT_GATE_TOTAL.get_metric_with_label_values(&[outcome]);
    }
    for outcome in [
        "applied",
        "already_applied",
        "failed",
        "uncertain_cleanup",
        "manifest_conflict",
    ] {
        let _ = FRONTEND_QUERY_LIFECYCLE_INIT.get_metric_with_label_values(&[outcome]);
    }
    for outcome in [
        "control_ready",
        "attach_failed",
        "heartbeat_timeout",
        "coordinator_lost",
        "local_failure",
        "backend_epoch_mismatch",
        "cleanup_failure",
        "terminal_locally_drained",
        "terminal_snapshot_accepted",
        "terminal_snapshot_idempotent",
        "terminal_snapshot_conflict",
        "terminal_finalize_failure",
    ] {
        let _ = FRONTEND_QUERY_LIFECYCLE_CONTROL.get_metric_with_label_values(&[outcome]);
    }
    for (phase, measure) in [
        ("init", "total"),
        ("init", "samples"),
        ("attach", "total"),
        ("attach", "samples"),
    ] {
        let _ = FRONTEND_QUERY_LIFECYCLE_LATENCY.get_metric_with_label_values(&[phase, measure]);
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn frontend_registry() -> Arc<FrontendMetricsRegistry> {
        FrontendMetricsRegistry::new().expect("create frontend metrics registry")
    }

    #[test]
    fn rendered_metrics_include_cluster_core_names() {
        observe_fragments_scheduled(1);
        observe_backend_heartbeat_rtt(Duration::from_millis(5));

        let registry = frontend_registry();
        let body = render_metrics(registry.as_ref()).expect("render metrics");
        assert!(body.contains("novarocks_fragment_scheduled_total"));
        assert!(body.contains("novarocks_heartbeat_rtt_seconds"));
        assert!(body.contains("novarocks_backend_registry_entries"));
        assert!(body.contains("novarocks_backend_eligible"));
        assert!(body.contains("novarocks_backend_announce_total"));
        assert!(body.contains("novarocks_backend_heartbeat_total"));
        assert!(body.contains("novarocks_pre_ready_replan_total"));
        assert!(body.contains("novarocks_pre_ready_effect_gate_total"));
        assert!(body.contains("novarocks_waiting_for_backend_seconds"));
        assert!(body.contains("novarocks_pre_ready_replan_seconds"));
    }

    #[test]
    fn frontend_registry_renders_dml_publication_collectors() {
        dml_publication::observe_terminal(
            novarocks_spi::connector::LakePublicationFamily::Ctas,
            crate::dml::attempt::DmlPublicationPhase::DispatchPossible,
            novarocks_spi::connector::LakePublicationDisposition::KnownCommitted,
            crate::dml::attempt::DmlPublicationFinalization::Failed,
        );

        let registry = frontend_registry();
        let body = render_metrics(registry.as_ref()).expect("render metrics");
        assert!(
            body.contains("novarocks_dml_publication_terminal_total"),
            "{body}"
        );
        let line = body
            .lines()
            .find(|line| {
                line.starts_with("novarocks_dml_publication_terminal_total")
                    && line.contains("family=\"ctas\"")
                    && line.contains("finalization=\"failed\"")
                    && line.contains("phase=\"dispatch_possible\"")
                    && line.contains("disposition=\"known_committed\"")
            })
            .expect("rendered CTAS finalization-failure metric");
        let value = line
            .rsplit_once(' ')
            .expect("metric line has a value")
            .1
            .parse::<u64>()
            .expect("metric value is an integer");
        assert!(value >= 1, "{line}");
    }

    #[test]
    fn backend_topology_gauges_preserve_the_last_nonzero_frontend_snapshot() {
        publish_backend_topology_metrics(11, 7, 6, 5, 4, 3, 2, 1, 9, 2, 5, 42);

        let registry = frontend_registry();
        let first = render_metrics(registry.as_ref()).expect("render first metrics snapshot");
        let second = render_metrics(registry.as_ref()).expect("render second metrics snapshot");

        for body in [&first, &second] {
            assert!(
                body.contains("novarocks_backend_registry_entries 11"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backend_announce_lease_valid 7"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backend_identity_verified 6"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backend_reported_state{state=\"running\"} 5"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backend_compatibility{state=\"compatible\"} 3"),
                "{body}"
            );
            assert!(
                body.contains("novarocks_backend_endpoint_ownership{state=\"owned\"} 9"),
                "{body}"
            );
            assert!(body.contains("novarocks_backend_eligible 5"), "{body}");
            assert!(
                body.contains("novarocks_backend_topology_revision 42"),
                "{body}"
            );
        }
    }

    #[test]
    fn rendered_json_metrics_are_fe_metrics_compatible_array() {
        observe_fragments_scheduled(1);
        let registry = frontend_registry();
        let body = render_metrics_json(registry.as_ref()).expect("render json metrics");
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
    fn frontend_process_query_counters_publish_structured_snapshot() {
        publish_frontend_process_query_counters(FrontendProcessQueryCountersSnapshot {
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
        });

        let registry = frontend_registry();
        let body =
            render_metrics(registry.as_ref()).expect("render frontend query lifecycle metrics");
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

    #[test]
    fn frontend_registry_excludes_foreign_process_collectors() {
        let foreign = prometheus::IntGauge::new(
            "novarocks_backend_foreign_registry_test_gauge",
            "Foreign collector used to prove Frontend registry isolation.",
        )
        .expect("create foreign collector");
        prometheus::default_registry()
            .register(Box::new(foreign))
            .expect("register foreign process collector");

        let registry = frontend_registry();
        let body = render_metrics(registry.as_ref()).expect("render frontend metrics");
        assert!(
            !body.contains("novarocks_backend_foreign_registry_test_gauge"),
            "Frontend endpoint must not gather process-global collector families: {body}"
        );
    }
}
