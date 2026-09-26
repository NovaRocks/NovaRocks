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

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::management_http::RoleMetricsRenderer;
use novarocks_worker::query_context::NativeQueryExecutionResourceSnapshot;
use once_cell::sync::Lazy;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntGaugeVec, Opts, Registry, TextEncoder,
};

/// Explicitly-owned Backend metric registry.  It is intentionally separate
/// from Prometheus' process-global registry so an all-in-one process cannot
/// leak a foreign role's metric families through the BE management endpoint.
pub struct BackendMetricsRegistry {
    registry: Registry,
    native_query_resources:
        Option<std::sync::Arc<dyn Fn() -> NativeQueryExecutionResourceSnapshot + Send + Sync>>,
    worker_reservations: Option<(
        std::sync::Arc<novarocks_worker::AdmissionReservationObservation>,
        usize,
    )>,
    worker_registry_lock: Option<std::sync::Arc<novarocks_worker::RegistryLockObservation>>,
}

// Native query resource gauges are process-global. Serialize their owner
// snapshot, gauge update, and collection across concurrent management scrapes.
static NATIVE_QUERY_RESOURCE_SCRAPE_LOCK: Mutex<()> = Mutex::new(());

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
            Box::new(Lazy::force(&BACKEND_NATIVE_INGRESS_SLOTS).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_INGRESS_WAIT_OLDEST_SINCE).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_INGRESS_LAST_PROGRESS).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_INGRESS_REJECTIONS).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_INGRESS_WAIT_MICROSECONDS).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_INGRESS_REQUEST_BYTES).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_INGRESS_FRAME_LIMIT_BYTES).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_RESPONSE_BACKINGS).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_CONTROL_QUEUE_WAIT).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_BLOCKING_QUEUE_WAIT).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_ASYNC_FIRST_POLL_LAG).clone()),
            Box::new(Lazy::force(&BACKEND_WORKER_CONTEXT_RESERVATIONS).clone()),
            Box::new(Lazy::force(&BACKEND_WORKER_RESERVATION_LAST_PUBLISHED).clone()),
            Box::new(Lazy::force(&BACKEND_WORKER_REGISTRY_LOCK).clone()),
            Box::new(Lazy::force(&BACKEND_WORKER_REGISTRY_LOCK_SAMPLE_AGE).clone()),
            Box::new(Lazy::force(&BACKEND_SATURATION_SOURCE_AVAILABLE).clone()),
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
        novarocks_execution::runtime::dispatch_metrics::register_driver_dispatch_metrics(
            &registry,
        )?;
        novarocks_execution::runtime::scan_stream_metrics::register_scan_stream_metrics(&registry)?;
        Ok(Self {
            registry,
            native_query_resources: None,
            worker_reservations: None,
            worker_registry_lock: None,
        })
    }

    pub fn with_native_query_resources(
        mut self,
        observation: std::sync::Arc<dyn Fn() -> NativeQueryExecutionResourceSnapshot + Send + Sync>,
    ) -> Self {
        self.native_query_resources = Some(observation);
        self
    }

    pub fn with_worker_reservations(
        mut self,
        observation: std::sync::Arc<novarocks_worker::AdmissionReservationObservation>,
        limit: usize,
    ) -> Self {
        self.worker_reservations = Some((observation, limit));
        self
    }

    pub fn with_worker_registry_lock(
        mut self,
        observation: std::sync::Arc<novarocks_worker::RegistryLockObservation>,
    ) -> Self {
        self.worker_registry_lock = Some(observation);
        self
    }

    fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        let _query_resource_scrape_guard = self.native_query_resources.as_ref().map(|_| {
            NATIVE_QUERY_RESOURCE_SCRAPE_LOCK
                .lock()
                .expect("native query resource scrape lock")
        });
        // Contexts can disappear in rollback and expiry paths. The owner is
        // the only source for these gauges, and the lock keeps concurrent
        // scrapes from publishing snapshots out of order.
        if let Some(observation) = &self.native_query_resources {
            let snapshot = observation();
            publish_backend_query_execution_resource(
                "native_query_contexts_active",
                snapshot.active_contexts,
            );
            publish_backend_query_execution_resource(
                "native_query_contexts_second_chance",
                snapshot.second_chance_contexts,
            );
            publish_backend_query_execution_resource(
                "native_query_active_fragments",
                snapshot.active_fragments,
            );
        }
        if let Some((observation, limit)) = &self.worker_reservations {
            publish_worker_context_reservation(
                observation.used(),
                *limit,
                observation.last_published_unix_seconds(),
            );
        }
        if let Some(observation) = &self.worker_registry_lock {
            publish_worker_registry_lock(observation.snapshot());
        }
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

static BACKEND_NATIVE_INGRESS_SLOTS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_native_ingress_slots",
            "Current and configured Native ingress slots by method class and phase.",
        ),
        &["class", "phase", "dimension"],
    )
    .expect("construct Native ingress slot gauge")
});

static BACKEND_NATIVE_INGRESS_WAIT_OLDEST_SINCE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_native_ingress_oldest_wait_since_unixtime_seconds",
            "Start time of the oldest currently waiting Native request, or zero when none.",
        ),
        &["class"],
    )
    .expect("construct Native oldest wait gauge")
});

static BACKEND_NATIVE_INGRESS_LAST_PROGRESS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_native_ingress_last_progress_unixtime_seconds",
            "Last Native gate transition, as Unix time seconds.",
        ),
        &["class"],
    )
    .expect("construct Native ingress progress gauge")
});

static BACKEND_NATIVE_INGRESS_REJECTIONS: Lazy<prometheus::IntCounterVec> = Lazy::new(|| {
    prometheus::IntCounterVec::new(
        Opts::new(
            "novarocks_backend_native_ingress_rejections_total",
            "Cumulative Native gate and frame rejections.",
        ),
        &["class", "reason"],
    )
    .expect("construct Native ingress rejection counter")
});

static BACKEND_NATIVE_INGRESS_WAIT_MICROSECONDS: Lazy<prometheus::IntCounterVec> =
    Lazy::new(|| {
        prometheus::IntCounterVec::new(
            Opts::new(
                "novarocks_backend_native_ingress_wait_microseconds_total",
                "Cumulative wait for a Native running slot, in microseconds.",
            ),
            &["class"],
        )
        .expect("construct Native ingress wait counter")
    });

static BACKEND_NATIVE_INGRESS_REQUEST_BYTES: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new("novarocks_backend_native_ingress_request_body_bytes", "Observed Native unary request body bytes before Prost construction; incomplete bodies are recorded separately.")
            .buckets(vec![4096.0, 65536.0, 1048576.0, 4194304.0, 16777216.0, 50331648.0, 67108869.0]),
        &["class", "outcome"],
    ).expect("construct Native request body histogram")
});

static BACKEND_NATIVE_INGRESS_FRAME_LIMIT_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("novarocks_backend_native_ingress_frame_limit_bytes", "Configured per-message decoded request limit; this is a rule, not concurrent utilization."),
        &["class"],
    ).expect("construct Native frame limit gauge")
});

static BACKEND_NATIVE_RESPONSE_BACKINGS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_native_response_backings",
            "Response bodies and handed-off DATA backings still retaining their ingress permit.",
        ),
        &["class", "kind"],
    )
    .expect("construct Native response backing gauge")
});

static BACKEND_NATIVE_CONTROL_QUEUE_WAIT: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "novarocks_backend_native_control_queue_wait_seconds",
            "Wait from bounded control executor enqueue to work start.",
        )
        .buckets(vec![0.0001, 0.001, 0.01, 0.1, 1.0, 10.0, 300.0]),
        &["outcome"],
    )
    .expect("construct Native control queue wait histogram")
});

static BACKEND_NATIVE_BLOCKING_QUEUE_WAIT: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "novarocks_backend_native_blocking_queue_wait_seconds",
            "Time from ordinary Native spawn_blocking submission to closure start.",
        )
        .buckets(vec![0.0001, 0.001, 0.01, 0.1, 1.0, 10.0, 300.0]),
        &["method"],
    )
    .expect("construct Native blocking queue wait histogram")
});

static BACKEND_NATIVE_ASYNC_FIRST_POLL_LAG: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "novarocks_backend_native_async_first_poll_lag_seconds",
            "Delay between the Native Tower call and first poll of its admission future.",
        )
        .buckets(vec![0.0001, 0.001, 0.01, 0.1, 1.0, 10.0]),
        &["class"],
    )
    .expect("construct Native async first-poll lag histogram")
});

static BACKEND_WORKER_CONTEXT_RESERVATIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("novarocks_backend_worker_context_reservations", "Worker-owned context reservations: atomic observation projection and configured limit."),
        &["dimension"],
    ).expect("construct Worker context reservation gauge")
});

static BACKEND_WORKER_RESERVATION_LAST_PUBLISHED: Lazy<prometheus::IntGauge> = Lazy::new(|| {
    prometheus::IntGauge::with_opts(Opts::new(
        "novarocks_backend_worker_reservation_last_published_unixtime_seconds",
        "Last Worker context-reservation observation publication, or zero if unavailable.",
    ))
    .expect("construct Worker reservation freshness gauge")
});

static BACKEND_WORKER_REGISTRY_LOCK: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_worker_registry_lock_observation",
            "Actual Worker registry mutex wait/hold samples, cumulative nanoseconds, and maximum nanoseconds. Intentional condition waits are excluded.",
        ),
        &["phase", "statistic"],
    )
    .expect("construct Worker registry lock gauge")
});

static BACKEND_WORKER_REGISTRY_LOCK_SAMPLE_AGE: Lazy<prometheus::IntGauge> = Lazy::new(|| {
    prometheus::IntGauge::with_opts(Opts::new(
        "novarocks_backend_worker_registry_lock_last_sample_age_nanoseconds",
        "Age of the last actual Worker registry lock sample, or -1 before any sample.",
    ))
    .expect("construct Worker registry lock freshness gauge")
});

static BACKEND_SATURATION_SOURCE_AVAILABLE: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new("novarocks_backend_saturation_source_available", "Whether a named local saturation source currently has an implemented readout; absent future owners are zero, not free capacity."),
        &["source"],
    ).expect("construct saturation source availability gauge")
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

fn unix_time_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs().min(i64::MAX as u64) as i64)
}

/// The Native listener owns these slots. Each transition is published at its
/// owner, so management collection never acquires the gate or Worker locks.
pub(crate) fn initialize_native_ingress_class(
    class: &'static str,
    running_limit: usize,
    waiting_limit: usize,
    request_limit: usize,
) {
    for (phase, limit) in [("running", running_limit), ("waiting", waiting_limit)] {
        BACKEND_NATIVE_INGRESS_SLOTS
            .with_label_values(&[class, phase, "limit"])
            .set(limit as i64);
        let _ = BACKEND_NATIVE_INGRESS_SLOTS.get_metric_with_label_values(&[class, phase, "used"]);
    }
    BACKEND_NATIVE_INGRESS_FRAME_LIMIT_BYTES
        .with_label_values(&[class])
        .set(request_limit as i64);
    let _ = BACKEND_NATIVE_INGRESS_WAIT_OLDEST_SINCE.get_metric_with_label_values(&[class]);
    let _ = BACKEND_NATIVE_INGRESS_LAST_PROGRESS.get_metric_with_label_values(&[class]);
    for kind in ["body", "data_backing"] {
        let _ = BACKEND_NATIVE_RESPONSE_BACKINGS.get_metric_with_label_values(&[class, kind]);
    }
    BACKEND_SATURATION_SOURCE_AVAILABLE
        .with_label_values(&["native_ingress"])
        .set(1);
}

pub(crate) fn native_ingress_slot_change(class: &'static str, phase: &'static str, delta: i64) {
    BACKEND_NATIVE_INGRESS_SLOTS
        .with_label_values(&[class, phase, "used"])
        .add(delta);
    BACKEND_NATIVE_INGRESS_LAST_PROGRESS
        .with_label_values(&[class])
        .set(unix_time_seconds());
}

pub(crate) fn native_ingress_oldest_wait_since(class: &'static str, since: i64) {
    BACKEND_NATIVE_INGRESS_WAIT_OLDEST_SINCE
        .with_label_values(&[class])
        .set(since);
}

pub(crate) fn native_ingress_waited(class: &'static str, duration: std::time::Duration) {
    BACKEND_NATIVE_INGRESS_WAIT_MICROSECONDS
        .with_label_values(&[class])
        .inc_by(duration.as_micros().min(u64::MAX as u128) as u64);
}

pub(crate) fn native_ingress_rejected(class: &'static str, reason: &'static str) {
    BACKEND_NATIVE_INGRESS_REJECTIONS
        .with_label_values(&[class, reason])
        .inc();
}

pub(crate) fn native_ingress_request_body_bytes(
    class: &'static str,
    outcome: &'static str,
    bytes: usize,
) {
    BACKEND_NATIVE_INGRESS_REQUEST_BYTES
        .with_label_values(&[class, outcome])
        .observe(bytes as f64);
}

pub(crate) fn native_response_backing_change(class: &'static str, kind: &'static str, delta: i64) {
    BACKEND_NATIVE_RESPONSE_BACKINGS
        .with_label_values(&[class, kind])
        .add(delta);
}

pub(crate) fn native_control_queue_wait(outcome: &'static str, duration: std::time::Duration) {
    BACKEND_NATIVE_CONTROL_QUEUE_WAIT
        .with_label_values(&[outcome])
        .observe(duration.as_secs_f64());
}

pub(crate) fn native_blocking_queue_wait(method: &'static str, duration: std::time::Duration) {
    BACKEND_NATIVE_BLOCKING_QUEUE_WAIT
        .with_label_values(&[method])
        .observe(duration.as_secs_f64());
}

pub(crate) fn native_async_first_poll_lag(class: &'static str, duration: std::time::Duration) {
    BACKEND_NATIVE_ASYNC_FIRST_POLL_LAG
        .with_label_values(&[class])
        .observe(duration.as_secs_f64());
}

pub(crate) fn publish_worker_context_reservation(
    used: usize,
    limit: usize,
    last_published_unix_seconds: u64,
) {
    BACKEND_WORKER_CONTEXT_RESERVATIONS
        .with_label_values(&["used"])
        .set(used as i64);
    BACKEND_WORKER_CONTEXT_RESERVATIONS
        .with_label_values(&["limit"])
        .set(limit as i64);
    BACKEND_WORKER_CONTEXT_RESERVATIONS
        .with_label_values(&["waiting"])
        .set(0);
    BACKEND_WORKER_RESERVATION_LAST_PUBLISHED
        .set(last_published_unix_seconds.min(i64::MAX as u64) as i64);
    BACKEND_SATURATION_SOURCE_AVAILABLE
        .with_label_values(&["worker_context_reservation"])
        .set(1);
}

fn publish_worker_registry_lock(snapshot: novarocks_worker::RegistryLockSnapshot) {
    for (phase, samples, total_nanos, max_nanos) in [
        (
            "wait",
            snapshot.wait_samples,
            snapshot.wait_nanoseconds,
            snapshot.wait_max_nanoseconds,
        ),
        (
            "hold",
            snapshot.hold_samples,
            snapshot.hold_nanoseconds,
            snapshot.hold_max_nanoseconds,
        ),
    ] {
        for (statistic, value) in [
            ("samples", samples),
            ("total_nanoseconds", total_nanos),
            ("max_nanoseconds", max_nanos),
        ] {
            BACKEND_WORKER_REGISTRY_LOCK
                .with_label_values(&[phase, statistic])
                .set(value.min(i64::MAX as u64) as i64);
        }
    }
    BACKEND_WORKER_REGISTRY_LOCK_SAMPLE_AGE.set(
        snapshot
            .sample_age_nanoseconds
            .map_or(-1, |age| age.min(i64::MAX as u64) as i64),
    );
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
    for source in [
        "native_ingress",
        "worker_context_reservation",
        "exchange_slot",
        "memory_charge",
    ] {
        let _ = BACKEND_SATURATION_SOURCE_AVAILABLE.get_metric_with_label_values(&[source]);
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
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Barrier};

    use prometheus::{IntGauge, Opts, Registry};

    use super::*;

    #[test]
    fn query_resource_scrape_replaces_stale_gauge() {
        let active = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&active);
        let backend = BackendMetricsRegistry::new()
            .expect("construct Backend registry")
            .with_native_query_resources(Arc::new(move || NativeQueryExecutionResourceSnapshot {
                active_contexts: observed.load(AtomicOrdering::Relaxed),
                second_chance_contexts: 0,
                active_fragments: 0,
            }));

        publish_backend_query_execution_resource("native_query_contexts_active", 1);
        let rendered = render_metrics(&backend).expect("render refreshed Backend metrics");
        assert!(rendered.contains(
            "novarocks_backend_query_execution_resources{resource=\"native_query_contexts_active\"} 0"
        ));

        active.store(2, AtomicOrdering::Relaxed);
        let rendered = render_metrics(&backend).expect("render changed Backend metrics");
        assert!(rendered.contains(
            "novarocks_backend_query_execution_resources{resource=\"native_query_contexts_active\"} 2"
        ));
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
        assert!(rendered.contains("novarocks_backend_query_execution_resources"));
        assert!(rendered.contains("novarocks_backend_task_execution_tasks_created_total"));
        assert!(rendered.contains("novarocks_exchange_shuffle_bytes_total"));
        assert!(!rendered.contains("novarocks_frontend_only_fixture"));
        assert!(
            rendered.contains(
                "novarocks_backend_saturation_source_available{source=\"exchange_slot\"} 0"
            )
        );
    }

    #[test]
    fn worker_projection_and_unavailable_exchange_are_rendered_in_both_formats() {
        let observation = Arc::new(novarocks_worker::AdmissionReservationObservation::default());
        let lock_observation = Arc::new(novarocks_worker::RegistryLockObservation::default());
        let backend = BackendMetricsRegistry::new()
            .expect("construct Backend registry")
            .with_worker_reservations(observation, 1024)
            .with_worker_registry_lock(lock_observation);
        let rendered = render_metrics(&backend).expect("render Backend metrics");
        assert!(
            rendered.contains("novarocks_backend_worker_context_reservations{dimension=\"used\"}")
        );
        assert!(
            rendered.contains("novarocks_backend_worker_context_reservations{dimension=\"limit\"}")
        );
        assert!(
            rendered
                .contains("novarocks_backend_worker_reservation_last_published_unixtime_seconds")
        );
        assert!(rendered.contains("novarocks_backend_worker_registry_lock_observation{phase=\"wait\",statistic=\"samples\"}"));
        assert!(
            rendered
                .contains("novarocks_backend_worker_registry_lock_last_sample_age_nanoseconds -1")
        );
        let json = render_metrics_json(&backend).expect("render Backend JSON");
        let rows: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        assert!(rows.as_array().is_some_and(|rows| rows.iter().any(|row| {
            row["tags"]["metric"] == "novarocks_backend_saturation_source_available"
                && row["tags"]["source"] == "exchange_slot"
                && row["value"].as_f64() == Some(0.0)
        })));
    }

    #[test]
    fn async_and_blocking_queue_samples_are_rendered() {
        native_async_first_poll_lag("ordinary", std::time::Duration::from_millis(2));
        native_blocking_queue_wait("apply_task_operations", std::time::Duration::from_millis(3));
        let backend = BackendMetricsRegistry::new().expect("construct Backend registry");
        let rendered = render_metrics(&backend).expect("render Backend metrics");
        assert!(rendered.contains(
            "novarocks_backend_native_async_first_poll_lag_seconds_count{class=\"ordinary\"}"
        ));
        assert!(rendered.contains("novarocks_backend_native_blocking_queue_wait_seconds_count{method=\"apply_task_operations\"}"));
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
