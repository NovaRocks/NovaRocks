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
use novarocks_memory::attribution::readout::{
    AttributionSnapshot, CLASS_LABELS, PRODUCTION_LABELS,
};
use novarocks_memory::observe::PhysicalMemoryReading;
use novarocks_worker::query_context::NativeQueryExecutionResourceSnapshot;
use once_cell::sync::Lazy;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
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
    process_memory: Option<(ProcessMemoryObservation, ProcessMemoryGauges)>,
    task_preparation: Option<std::sync::Arc<novarocks_worker::TaskExecutionRegistry>>,
    task_preparation_gauges: IntGaugeVec,
    preparation_snapshot_available: IntGauge,
    root_ownership_gauges: IntGaugeVec,
    root_snapshot_available: IntGauge,
    scrape_lock: std::sync::Mutex<()>,
}

/// What the Backend `/metrics` endpoint reports about this process's memory.
///
/// Supplied by the process that owns the allocator and the probes; this crate
/// registers and renders the readings without knowing whether they came from
/// a cgroup, the resident set or an allocator. Each source is its own family
/// because the sources overlap and must never be summed.
#[derive(Clone)]
pub struct ProcessMemoryObservation {
    /// The allocator that serves this process, as a stable label.
    pub allocator: &'static str,
    /// Allocator settings that took effect, by stable name; empty when the
    /// allocator publishes none.
    pub allocator_settings: Vec<(&'static str, i64)>,
    /// What bounds this process's memory — its bytes, and `"cgroup"` or
    /// `"physical"` for what binds — or `None` when neither is known.
    pub visible_memory: Option<(u64, &'static str)>,
    /// Sampled on every scrape.
    pub sample:
        std::sync::Arc<dyn Fn() -> (AttributionSnapshot, PhysicalMemoryReading) + Send + Sync>,
}

/// The gauges refreshed from [`ProcessMemoryObservation::sample`]. Owned by
/// one registry, so two registries in one process never share readings.
struct ProcessMemoryGauges {
    physical: IntGaugeVec,
    allocator: IntGaugeVec,
    counted_live: IntGaugeVec,
    attribution: AttributionGauges,
}

/// Absolute samples are published under the registry's scrape lock. No label
/// contains an account, query, record index, generation or owner identity.
struct AttributionGauges {
    operations: IntCounterVec,
    requested: IntCounterVec,
    attributed: IntGaugeVec,
    records: IntGaugeVec,
    faults: IntCounterVec,
    unattributed: IntGaugeVec,
    blind_spot: IntGaugeVec,
    reconciliation: IntGaugeVec,
    capacity: IntGaugeVec,
    high_water: IntGaugeVec,
    draining: IntGaugeVec,
    segment_requested: IntGaugeVec,
    metadata: IntGaugeVec,
    batch_threshold: IntGaugeVec,
    pinned_slots: IntGaugeVec,
    slot_estimate: IntGaugeVec,
    sampled_at: IntGaugeVec,
    sequence: IntGaugeVec,
}
impl AttributionGauges {
    fn new(registry: &Registry) -> Result<Self, String> {
        fn gauge(
            registry: &Registry,
            name: &str,
            help: &str,
            labels: &[&str],
        ) -> Result<IntGaugeVec, String> {
            let value = IntGaugeVec::new(Opts::new(name, help), labels)
                .map_err(|e| format!("construct {name}: {e}"))?;
            registry
                .register(Box::new(value.clone()))
                .map_err(|e| format!("register {name}: {e}"))?;
            Ok(value)
        }
        fn counter(
            registry: &Registry,
            name: &str,
            help: &str,
            labels: &[&str],
        ) -> Result<IntCounterVec, String> {
            let value = IntCounterVec::new(Opts::new(name, help), labels)
                .map_err(|e| format!("construct {name}: {e}"))?;
            registry
                .register(Box::new(value.clone()))
                .map_err(|e| format!("register {name}: {e}"))?;
            Ok(value)
        }
        Ok(Self {
            operations: counter(
                registry,
                "novarocks_backend_process_counted_operations_total",
                "Successful alloc, dealloc and realloc calls and failed requests by size band. Cross-band resize is one realloc and never a fabricated alloc/free pair.",
                &["band", "kind"],
            )?,
            requested: counter(
                registry,
                "novarocks_backend_process_counted_requested_bytes_total",
                "Cumulative requested byte flows within each band, including whole-block cross-band migration; token bytes are included. Band flows are not the delta-only process total.",
                &["band", "flow"],
            )?,
            attributed: gauge(
                registry,
                "novarocks_backend_memory_attributed_bytes",
                "Signed independent lane samples by fact band and responsibility class. Pinned batching can temporarily make samples negative; these are not physical measurements.",
                &["band", "class"],
            )?,
            records: gauge(
                registry,
                "novarocks_backend_memory_lane_records",
                "Record counts from a bounded prefix sample by responsibility and production state; not an instantaneous coherent snapshot.",
                &["class", "production"],
            )?,
            faults: counter(
                registry,
                "novarocks_backend_memory_attribution_faults_total",
                "Cumulative attribution diagnostics sampled from fixed shards; observation never rejects SQL.",
                &["kind"],
            )?,
            unattributed: gauge(
                registry,
                "novarocks_backend_memory_unattributed_bytes",
                "Signed tagged bytes in immortal unattributed source records.",
                &[],
            )?,
            blind_spot: gauge(
                registry,
                "novarocks_backend_memory_ledger_blind_spot_bytes",
                "Signed small-band live request bytes minus explicit R1 small facts; independent samples may transiently be negative.",
                &[],
            )?,
            reconciliation: gauge(
                registry,
                "novarocks_backend_memory_attribution_reconcile_bytes",
                "Signed tagged-band live request bytes minus tagged lane facts including unattributed. Pending slot balances and in-flight events can explain a nonzero sample; this is not a settlement proof.",
                &[],
            )?,
            capacity: gauge(
                registry,
                "novarocks_backend_memory_lane_record_capacity",
                "Maximum record-store positions, including immortal unattributed records.",
                &[],
            )?,
            high_water: gauge(
                registry,
                "novarocks_backend_memory_lane_record_high_water",
                "Sampled occupied-prefix bound for record-store observation.",
                &[],
            )?,
            draining: gauge(
                registry,
                "novarocks_backend_memory_lane_records_draining",
                "Hook-external draining queue length sampled independently.",
                &[],
            )?,
            segment_requested: gauge(
                registry,
                "novarocks_backend_memory_lane_record_segment_requested_bytes",
                "Requested record segment backing represented by sampled high-water; excludes inline/control storage and is not resident memory.",
                &[],
            )?,
            metadata: gauge(
                registry,
                "novarocks_backend_memory_observation_metadata_bytes",
                "This BE authority's observation-control storage estimate (registry backing and record/lane/member fees), absent when unsupplied. S1 excludes this diagnostic from capacity C.",
                &[],
            )?,
            batch_threshold: gauge(
                registry,
                "novarocks_backend_memory_batch_threshold_bytes",
                "Frozen per-slot batching quantum Q.",
                &[],
            )?,
            pinned_slots: gauge(
                registry,
                "novarocks_backend_memory_batch_pinned_slots",
                "Approximate independently sampled slot pins; not an instantaneous concurrent bound.",
                &[],
            )?,
            slot_estimate: gauge(
                registry,
                "novarocks_backend_memory_batch_slot_balance_estimate_bytes",
                "Q times sampled pins, excluding in-flight allocations. This estimate is not an instantaneous or concurrent mathematical upper bound and cannot establish settlement.",
                &[],
            )?,
            sampled_at: gauge(
                registry,
                "novarocks_backend_memory_attribution_sample_unixtime_seconds",
                "Attribution sample time, absent when wall-clock time is unavailable; not a coherent snapshot timestamp.",
                &[],
            )?,
            sequence: gauge(
                registry,
                "novarocks_backend_memory_attribution_sequence_sum",
                "Wrapping sum of independently sampled lane publication sequences; not a settlement or freshness proof.",
                &[],
            )?,
        })
    }
    fn publish(&self, sample: AttributionSnapshot) {
        fn absolute(counter: &IntCounterVec, labels: &[&str], value: u64) {
            // These collectors are private to this registry. Its scrape lock
            // covers reset, absolute publication and collection as one operation.
            let metric = counter.with_label_values(labels);
            metric.reset();
            metric.inc_by(value);
        }
        fn scalar(gauge: &IntGaugeVec, value: i128) {
            gauge.with_label_values(&[]).set(signed_gauge_value(value));
        }
        fn optional(gauge: &IntGaugeVec, value: Option<u64>) {
            if let Some(value) = value {
                gauge.with_label_values(&[]).set(gauge_value(value));
            } else {
                let _ = gauge.remove_label_values(&[]);
            }
        }
        for (band, count, frees) in [
            (
                "small",
                sample.process.small,
                sample.process.small_deallocations,
            ),
            (
                "tagged",
                sample.process.tagged,
                sample.process.tagged_deallocations,
            ),
        ] {
            for (kind, value) in [
                ("alloc", count.allocations),
                ("dealloc", frees),
                ("realloc", count.reallocations),
                ("failure", count.failures),
            ] {
                absolute(&self.operations, &[band, kind], value);
            }
            for (flow, value) in [
                ("allocated", count.allocated_total_bytes),
                ("deallocated", count.deallocated_total_bytes),
            ] {
                absolute(&self.requested, &[band, flow], value);
            }
        }
        for (index, facts) in sample.classified.iter().enumerate() {
            for (band, bytes) in [
                ("tagged", facts.tagged_bytes),
                ("r1_small", facts.r1_small_bytes),
            ] {
                self.attributed
                    .with_label_values(&[band, CLASS_LABELS[index]])
                    .set(signed_gauge_value(bytes));
            }
        }
        for (class, counts) in sample.records.iter().enumerate() {
            for (production, count) in counts.iter().enumerate() {
                self.records
                    .with_label_values(&[CLASS_LABELS[class], PRODUCTION_LABELS[production]])
                    .set(gauge_value(*count));
            }
        }
        let faults = sample.faults;
        for (kind, count) in [
            ("binding_failure", faults.binding_failures),
            ("record_exhaustion", faults.record_exhaustions),
            ("orphan", faults.orphan_events),
            ("residual_growth", faults.residual_growth_events),
            ("scope_refusal", faults.scope_refusals),
            ("reclaim_nonzero", faults.reclaim_nonzero_events),
            ("generation_exhaustion", faults.generation_exhaustions),
        ] {
            absolute(&self.faults, &[kind], count);
        }
        scalar(&self.unattributed, sample.unattributed_tagged_bytes);
        scalar(&self.blind_spot, sample.ledger_blind_spot_bytes);
        scalar(&self.reconciliation, sample.tagged_reconciliation_bytes);
        scalar(&self.capacity, i128::from(sample.record_capacity));
        scalar(&self.high_water, i128::from(sample.record_high_water));
        scalar(&self.draining, sample.draining_records as i128);
        scalar(
            &self.segment_requested,
            i128::from(sample.record_segment_requested_bytes),
        );
        optional(&self.metadata, sample.observation_metadata_bytes);
        scalar(
            &self.batch_threshold,
            i128::from(sample.batch_threshold_bytes),
        );
        scalar(&self.pinned_slots, i128::from(sample.faults.pinned_slots));
        scalar(
            &self.slot_estimate,
            i128::from(sample.slot_balance_estimate_excluding_in_flight_bytes),
        );
        optional(
            &self.sampled_at,
            sample.sampled_at_unix_millis.map(|millis| millis / 1000),
        );
        scalar(&self.sequence, i128::from(sample.sequence_sum));
    }
}
fn signed_gauge_value(bytes: i128) -> i64 {
    bytes.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

// Native query resource gauges are process-global. Serialize their owner
// snapshot, gauge update, and collection across concurrent management scrapes.
pub(crate) static NATIVE_QUERY_RESOURCE_SCRAPE_LOCK: Mutex<()> = Mutex::new(());

impl BackendMetricsRegistry {
    pub fn new() -> Result<Self, String> {
        let registry = Registry::new();
        let task_preparation_gauges = IntGaugeVec::new(
            Opts::new("novarocks_backend_task_preparation", "Temporary preparation ownership and configured limits from the Worker charge ledger."),
            &["resource", "dimension"],
        ).map_err(|error| format!("construct task preparation metrics: {error}"))?;
        registry
            .register(Box::new(task_preparation_gauges.clone()))
            .map_err(|error| format!("register task preparation metrics: {error}"))?;
        let preparation_snapshot_available = IntGauge::with_opts(Opts::new(
            "novarocks_backend_task_preparation_snapshot_available",
            "Whether this scrape obtained the exact Worker preparation charge ledger.",
        ))
        .map_err(|error| format!("construct preparation snapshot availability: {error}"))?;
        registry
            .register(Box::new(preparation_snapshot_available.clone()))
            .map_err(|error| format!("register preparation snapshot availability: {error}"))?;
        let root_ownership_gauges = IntGaugeVec::new(
            Opts::new("novarocks_backend_root_ownership", "Bounded census of context-owned root channels. Logical state is lock-consistent per channel; physical owner counters are independent samples. Released channels and other Arc tails are excluded; zero is not a last-owner exit receipt."),
            &["resource"],
        ).map_err(|error| format!("construct root ownership metrics: {error}"))?;
        registry
            .register(Box::new(root_ownership_gauges.clone()))
            .map_err(|error| format!("register root ownership metrics: {error}"))?;
        let root_snapshot_available = IntGauge::with_opts(Opts::new(
            "novarocks_backend_root_ownership_snapshot_available",
            "Whether this scrape completed the bounded nonblocking Worker root census. Busy locks or insufficient scan coverage are unavailable, never zero usage.",
        )).map_err(|error| format!("construct root snapshot availability: {error}"))?;
        registry
            .register(Box::new(root_snapshot_available.clone()))
            .map_err(|error| format!("register root snapshot availability: {error}"))?;
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
            Box::new(Lazy::force(&BACKEND_NATIVE_TRANSPORT_POSITIONS).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_TRANSPORT_REFUSED).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_LANE_CONNECTIONS).clone()),
            Box::new(Lazy::force(&BACKEND_NATIVE_LANE_STREAMS).clone()),
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
            process_memory: None,
            task_preparation: None,
            task_preparation_gauges,
            preparation_snapshot_available,
            root_ownership_gauges,
            root_snapshot_available,
            scrape_lock: std::sync::Mutex::new(()),
        })
    }

    /// Registers this process's memory readings: the allocator and its
    /// settings and the visible memory once, the physical readings on every
    /// scrape. A reading the observation cannot provide is not exported.
    pub fn with_process_memory(
        mut self,
        observation: ProcessMemoryObservation,
    ) -> Result<Self, String> {
        let registry = &self.registry;
        let gauge_vec = |name: &str, help: &str, label: &str| -> Result<IntGaugeVec, String> {
            let gauge = IntGaugeVec::new(Opts::new(name, help), &[label])
                .map_err(|error| format!("construct {name}: {error}"))?;
            registry
                .register(Box::new(gauge.clone()))
                .map_err(|error| format!("register {name}: {error}"))?;
            Ok(gauge)
        };
        let visible = gauge_vec(
            "novarocks_backend_process_visible_memory_bytes",
            "The memory bound this Backend process plans against, labelled by what binds it: \
             the cgroup limit or physical memory.",
            "bound",
        )?;
        let allocator_info = gauge_vec(
            "novarocks_backend_process_allocator_info",
            "The allocator serving this Backend process; the value is always 1.",
            "allocator",
        )?;
        let allocator_setting = gauge_vec(
            "novarocks_backend_process_allocator_setting",
            "Allocator settings that took effect in this Backend process, one series per \
             setting; absent when the allocator publishes none.",
            "setting",
        )?;
        let physical = gauge_vec(
            "novarocks_backend_process_physical_memory_bytes",
            "Physical memory of this Backend process as the operating system reports it, one \
             series per source. Sources overlap and are not additive; a source that cannot be \
             read is absent.",
            "source",
        )?;
        let allocator = gauge_vec(
            "novarocks_backend_process_allocator_memory_bytes",
            "The process allocator's own statistics, one series per statistic. Statistics \
             overlap and are not additive; they are absent when the allocator publishes none.",
            "statistic",
        )?;
        let counted_live = gauge_vec(
            "novarocks_backend_process_counted_live_bytes",
            "Live inner requested bytes by user-layout size band; tagged includes source-token bytes. Independent counter samples are not physical measurements.",
            "band",
        )?;
        let attribution = AttributionGauges::new(registry)?;

        if let Some((bytes, bound)) = observation.visible_memory {
            visible.with_label_values(&[bound]).set(gauge_value(bytes));
        }
        allocator_info
            .with_label_values(&[observation.allocator])
            .set(1);
        for (setting, value) in &observation.allocator_settings {
            allocator_setting.with_label_values(&[setting]).set(*value);
        }
        self.process_memory = Some((
            observation,
            ProcessMemoryGauges {
                physical,
                allocator,
                counted_live,
                attribution,
            },
        ));
        Ok(self)
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

    pub fn with_task_preparation(
        mut self,
        owner: std::sync::Arc<novarocks_worker::TaskExecutionRegistry>,
    ) -> Self {
        self.task_preparation = Some(owner);
        self
    }

    fn gather(&self) -> Result<Vec<prometheus::proto::MetricFamily>, String> {
        // Keep owner sampling, gauge projection and collection in one scrape order.
        let _scrape = self.scrape_lock.lock().expect("backend metric scrape lock");
        let preparation = self
            .task_preparation
            .as_ref()
            .map(|owner| owner.try_preparation_snapshot())
            .transpose()
            .map_err(|error| format!("sample Worker preparation charge ledger: {error}"))?
            .flatten();
        self.preparation_snapshot_available
            .set(i64::from(preparation.is_some()));
        if let Some(snapshot) = preparation {
            for (resource, used, limit) in [
                ("positions", snapshot.positions, snapshot.position_limit),
                (
                    "context_positions",
                    snapshot.context_positions,
                    snapshot.context_position_limit,
                ),
                (
                    "queued_positions",
                    snapshot.queued_positions,
                    snapshot.position_limit,
                ),
                ("workers", snapshot.workers, snapshot.worker_limit),
                ("bytes", snapshot.bytes, snapshot.byte_limit),
            ] {
                self.task_preparation_gauges
                    .with_label_values(&[resource, "used"])
                    .set(i64::try_from(used).unwrap_or(i64::MAX));
                self.task_preparation_gauges
                    .with_label_values(&[resource, "limit"])
                    .set(i64::try_from(limit).unwrap_or(i64::MAX));
            }
        }
        let roots = self
            .task_preparation
            .as_ref()
            .map(|owner| owner.try_root_ownership_snapshot())
            .transpose()
            .map_err(|error| format!("sample Worker root ownership: {error}"))?
            .flatten();
        self.root_snapshot_available.set(i64::from(roots.is_some()));
        if let Some(snapshot) = roots {
            for (resource, used) in [
                ("channels", snapshot.channels),
                ("terminal_task_records", snapshot.terminal_task_records),
                ("producers_running", snapshot.producers_running),
                ("producers_exited", snapshot.producers_exited),
                ("ends_published", snapshot.ends_published),
                ("ends_acknowledged", snapshot.ends_acknowledged),
                ("sealed", snapshot.sealed),
                ("data_positions", snapshot.data_positions),
                ("payload_bytes", snapshot.payload_bytes),
                ("segments", snapshot.segments),
                ("deliveries", snapshot.deliveries),
                ("retained_reservations", snapshot.retained_reservations),
                ("metadata_holders", snapshot.metadata_holders),
                ("metadata_bytes", snapshot.metadata_bytes),
            ] {
                self.root_ownership_gauges
                    .with_label_values(&[resource])
                    .set(i64::try_from(used).unwrap_or(i64::MAX));
            }
        }
        let _query_resource_scrape_guard = self.native_query_resources.as_ref().map(|_| {
            NATIVE_QUERY_RESOURCE_SCRAPE_LOCK
                .lock()
                .expect("native query resource scrape lock")
        });
        if let Some((observation, gauges)) = &self.process_memory {
            let (counted, physical) = (observation.sample)();
            publish_process_memory(gauges, counted, physical);
        }
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
        let mut families = self.registry.gather();
        if preparation.is_none() {
            // This scrape has no exact ledger. Previously sampled values must
            // not appear as current values, and unavailable is not zero usage.
            families.retain(|family| family.get_name() != "novarocks_backend_task_preparation");
        }
        if roots.is_none() {
            families.retain(|family| family.get_name() != "novarocks_backend_root_ownership");
        }
        Ok(families)
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

static BACKEND_NATIVE_TRANSPORT_POSITIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_native_transport_positions",
            "Native connection admission positions by class and kind: live connections \
             (until their IO is dropped) and bootstrapping handshakes, used and limit.",
        ),
        &["class", "kind", "dimension"],
    )
    .expect("construct Native transport position gauge")
});

static BACKEND_NATIVE_TRANSPORT_REFUSED: Lazy<prometheus::IntCounterVec> = Lazy::new(|| {
    prometheus::IntCounterVec::new(
        Opts::new(
            "novarocks_backend_native_transport_refused_connections_total",
            "Native connections accepted or dialed and refused for lack of a position.",
        ),
        &["class"],
    )
    .expect("construct Native transport refusal counter")
});

static BACKEND_NATIVE_LANE_CONNECTIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_native_lane_connections",
            "Live Native connections bound to a lane: sealed incoming and established outgoing.",
        ),
        &["lane"],
    )
    .expect("construct Native lane connection gauge")
});

static BACKEND_NATIVE_LANE_STREAMS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_backend_native_lane_streams",
            "Native stream positions by lane and direction, held until the response body ends \
             or is dropped; used and limit (an outgoing limit is per connection).",
        ),
        &["lane", "direction", "dimension"],
    )
    .expect("construct Native lane stream gauge")
});

/// Publishes the Backend's Native admission transitions. It only writes
/// gauges; admission never reads them.
#[derive(Debug, Default)]
pub struct BackendNativeTransportMetrics;

impl crate::native_lane::NativeTransportObserver for BackendNativeTransportMetrics {
    fn positions(
        &self,
        class: crate::native_transport_admission::TransportClass,
        kind: crate::native_lane::PositionKind,
        used: usize,
        limit: usize,
    ) {
        for (dimension, value) in [("used", used), ("limit", limit)] {
            BACKEND_NATIVE_TRANSPORT_POSITIONS
                .with_label_values(&[class.label(), kind.label(), dimension])
                .set(i64::try_from(value).unwrap_or(i64::MAX));
        }
    }

    fn refused(&self, class: crate::native_transport_admission::TransportClass) {
        BACKEND_NATIVE_TRANSPORT_REFUSED
            .with_label_values(&[class.label()])
            .inc();
    }

    fn lane_connections(&self, lane: crate::native_lane::NativeLane, delta: i64) {
        BACKEND_NATIVE_LANE_CONNECTIONS
            .with_label_values(&[lane.label()])
            .add(delta);
    }

    fn lane_streams(
        &self,
        lane: crate::native_lane::NativeLane,
        direction: crate::native_lane::StreamDirection,
        delta: i64,
    ) {
        BACKEND_NATIVE_LANE_STREAMS
            .with_label_values(&[lane.label(), direction.label(), "used"])
            .add(delta);
    }

    fn lane_stream_limit(
        &self,
        lane: crate::native_lane::NativeLane,
        direction: crate::native_lane::StreamDirection,
        limit: usize,
    ) {
        BACKEND_NATIVE_LANE_STREAMS
            .with_label_values(&[lane.label(), direction.label(), "limit"])
            .set(i64::try_from(limit).unwrap_or(i64::MAX));
    }
}

/// The observer a Backend installs on its process transport admission.
pub fn backend_native_transport_observer()
-> std::sync::Arc<dyn crate::native_lane::NativeTransportObserver> {
    std::sync::Arc::new(BackendNativeTransportMetrics)
}

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

fn publish_process_memory(
    gauges: &ProcessMemoryGauges,
    counted: AttributionSnapshot,
    physical: PhysicalMemoryReading,
) {
    fn set_or_remove(gauge: &IntGaugeVec, label: &str, value: Option<u64>) {
        match value {
            Some(bytes) => gauge.with_label_values(&[label]).set(gauge_value(bytes)),
            // Absent, not zero: nothing measured this source on this scrape.
            None => {
                let _ = gauge.remove_label_values(&[label]);
            }
        }
    }
    gauges
        .counted_live
        .with_label_values(&["small"])
        .set(gauge_value(counted.process.small.live_bytes));
    gauges
        .counted_live
        .with_label_values(&["tagged"])
        .set(gauge_value(counted.process.tagged.live_bytes));
    gauges.attribution.publish(counted);
    set_or_remove(
        &gauges.physical,
        "cgroup_anonymous",
        physical.cgroup_anonymous_bytes,
    );
    set_or_remove(
        &gauges.physical,
        "process_resident",
        physical.process_resident_bytes,
    );
    let internals = physical.allocator_internals;
    set_or_remove(
        &gauges.allocator,
        "allocated",
        internals.map(|reading| reading.allocated_bytes),
    );
    set_or_remove(
        &gauges.allocator,
        "active",
        internals.map(|reading| reading.active_bytes),
    );
    set_or_remove(
        &gauges.allocator,
        "resident",
        internals.map(|reading| reading.resident_bytes),
    );
}

/// Byte counts fit an `i64` gauge on any real machine; saturate rather than
/// wrap if one ever does not.
fn gauge_value(bytes: u64) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}

/// Renders only the metric families registered by this Backend role.
pub(crate) fn render_metrics(metrics: &BackendMetricsRegistry) -> Result<String, String> {
    refresh_backend_gauges();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&metrics.gather()?, &mut buf)
        .map_err(|e| format!("encode prometheus metrics failed: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("prometheus metrics were not utf-8: {e}"))
}

/// Renders only the role-owned metrics in the existing JSON form.
pub(crate) fn render_metrics_json(metrics: &BackendMetricsRegistry) -> Result<String, String> {
    refresh_backend_gauges();
    let mut rows = Vec::new();
    for family in metrics.gather()? {
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
    for lane in crate::native_lane::NativeLane::ALL {
        let _ = BACKEND_NATIVE_LANE_CONNECTIONS.get_metric_with_label_values(&[lane.label()]);
    }
    for class in ["data", "control"] {
        let _ = BACKEND_NATIVE_TRANSPORT_REFUSED.get_metric_with_label_values(&[class]);
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

    struct UnusedPreparationHost;

    impl novarocks_worker::QueryContextHost for UnusedPreparationHost {
        fn materialize(
            &self,
            _: novarocks_worker::SharedFactsRequest<'_>,
        ) -> Result<(), novarocks_worker::HostRejection> {
            panic!("metrics fixture must not materialize a context")
        }
        fn release(
            &self,
            _: novarocks_execution_contract::QueryContextRef,
        ) -> novarocks_worker::ReleasedContextEvidence {
            panic!("metrics fixture must not release a context")
        }
        fn advance_shared_domain(
            &self,
            _: novarocks_execution_contract::QueryContextRef,
            _: &novarocks_execution_contract::task_execution::operation::QueryContextDomainUpdate,
        ) -> Result<(), novarocks_worker::HostRejection> {
            panic!("metrics fixture must not advance a context")
        }
    }

    impl novarocks_worker::TaskExecutionHost for UnusedPreparationHost {
        fn close_context_admission(&self, _: novarocks_execution_contract::QueryContextRef) {}
        fn retire_context_execution(&self, _: novarocks_execution_contract::QueryContextRef) {}
        fn forget_context_admission(&self, _: novarocks_execution_contract::QueryContextRef) {}
        fn install_receiver(
            &self,
            _: &novarocks_execution_contract::task_execution::descriptor::TaskDescriptor,
            _: novarocks_execution_contract::task_execution::creation::TaskCreationInput,
            _preparation: &novarocks_worker::PreparationControlLoan<'_>,
        ) -> Result<novarocks_worker::PreparedTaskInstallation, novarocks_worker::HostRejection>
        {
            panic!("metrics fixture must not install a task")
        }
        fn remove_receiver(
            &self,
            _: &novarocks_execution_contract::task_execution::descriptor::TaskDescriptor,
        ) {
        }
        fn install_inbound_capability(
            &self,
            _: &novarocks_execution_contract::task_execution::descriptor::TaskDescriptor,
        ) -> Result<(), novarocks_worker::HostRejection> {
            panic!("metrics fixture must not install an inbound capability")
        }
        fn remove_inbound_capability(
            &self,
            _: &novarocks_execution_contract::task_execution::descriptor::TaskDescriptor,
        ) {
        }
        fn submit_runnable(
            &self,
            _: &novarocks_execution_contract::task_execution::descriptor::TaskDescriptor,
            _: novarocks_worker::TaskStatusReporter,
        ) -> Result<Arc<dyn novarocks_worker::RunnableTask>, novarocks_worker::HostRejection>
        {
            panic!("metrics fixture must not submit a task")
        }
        fn apply_task_domain(
            &self,
            _: &novarocks_execution_contract::task_execution::descriptor::TaskDescriptor,
            _: &novarocks_execution_contract::task_execution::operation::TaskDomainUpdate,
        ) -> Result<Option<u64>, novarocks_worker::HostRejection> {
            panic!("metrics fixture must not advance a task")
        }
    }

    fn preparation_owner() -> Arc<novarocks_worker::TaskExecutionRegistry> {
        novarocks_worker::TaskExecutionRegistry::new(
            novarocks_worker::TaskExecutionRegistryConfig::for_process(
                novarocks_types::BackendProcessId::new_v7(),
                17,
                29,
            ),
            Arc::new(novarocks_worker::ManualClock::new()),
            Arc::new(UnusedPreparationHost),
            Arc::new(UnusedPreparationHost),
            crate::task_execution_observation::backend_task_execution_ports(),
        )
    }

    #[cfg(debug_assertions)]
    #[test]
    fn root_ownership_scrape_omits_stale_or_absent_samples_in_both_formats() {
        let absent = BackendMetricsRegistry::new().unwrap();
        let absent_text = render_metrics(&absent).unwrap();
        assert!(absent_text.contains("novarocks_backend_root_ownership_snapshot_available 0"));
        assert!(!absent_text.contains("novarocks_backend_root_ownership{"));
        let owner = preparation_owner();
        let backend = BackendMetricsRegistry::new()
            .unwrap()
            .with_task_preparation(Arc::clone(&owner));
        for (render, is_text) in [
            (
                render_metrics as fn(&BackendMetricsRegistry) -> Result<String, String>,
                true,
            ),
            (render_metrics_json, false),
        ] {
            let text = render(&backend).unwrap();
            assert!(text.contains("novarocks_backend_root_ownership"));
            owner.with_registry_lock_for_test(|| {
                let text = render(&backend).unwrap();
                if is_text {
                    assert!(text.contains("novarocks_backend_root_ownership_snapshot_available 0"));
                    assert!(!text.contains("novarocks_backend_root_ownership{"));
                } else {
                    let rows: serde_json::Value = serde_json::from_str(&text).unwrap();
                    assert!(
                        !rows
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|row| row["tags"]["metric"] == "novarocks_backend_root_ownership")
                    );
                    assert!(
                        rows.as_array()
                            .unwrap()
                            .iter()
                            .any(|row| row["tags"]["metric"]
                                == "novarocks_backend_root_ownership_snapshot_available"
                                && row["value"].as_f64() == Some(0.0))
                    );
                }
            });
        }
        assert!(
            render_metrics(&backend)
                .unwrap()
                .contains("novarocks_backend_root_ownership_snapshot_available 1")
        );
    }

    fn assert_preparation_unavailable(prometheus: &str, json: &str) {
        assert!(prometheus.contains("novarocks_backend_task_preparation_snapshot_available 0"));
        assert!(
            !prometheus.contains("novarocks_backend_task_preparation{"),
            "{prometheus}"
        );
        let rows: serde_json::Value = serde_json::from_str(json).expect("metrics JSON");
        let rows = rows.as_array().expect("metric rows");
        assert!(rows.iter().any(|row| row["tags"]["metric"]
            == "novarocks_backend_task_preparation_snapshot_available"
            && row["value"].as_f64() == Some(0.0)));
        assert!(
            !rows
                .iter()
                .any(|row| row["tags"]["metric"] == "novarocks_backend_task_preparation")
        );
    }

    #[test]
    fn preparation_without_owner_is_unavailable_in_both_formats() {
        let backend = BackendMetricsRegistry::new().expect("Backend metrics");
        backend
            .task_preparation_gauges
            .with_label_values(&["positions", "used"])
            .set(123);
        assert_preparation_unavailable(
            &render_metrics(&backend).unwrap(),
            &render_metrics_json(&backend).unwrap(),
        );
    }

    #[test]
    fn preparation_projects_exact_owner_ledger_in_both_formats() {
        let owner = preparation_owner();
        let snapshot = owner.preparation_snapshot();
        let backend = BackendMetricsRegistry::new()
            .expect("Backend metrics")
            .with_task_preparation(owner);
        backend
            .task_preparation_gauges
            .with_label_values(&["positions", "used"])
            .set(123);
        let prometheus = render_metrics(&backend).expect("exact preparation scrape");
        let json = render_metrics_json(&backend).expect("exact preparation JSON");
        assert!(prometheus.contains("novarocks_backend_task_preparation_snapshot_available 1"));
        let rows: serde_json::Value = serde_json::from_str(&json).unwrap();
        let rows = rows.as_array().unwrap();
        assert!(rows.iter().any(|row| row["tags"]["metric"]
            == "novarocks_backend_task_preparation_snapshot_available"
            && row["value"].as_f64() == Some(1.0)));
        for (resource, used, limit) in [
            ("positions", snapshot.positions, snapshot.position_limit),
            (
                "context_positions",
                snapshot.context_positions,
                snapshot.context_position_limit,
            ),
            (
                "queued_positions",
                snapshot.queued_positions,
                snapshot.position_limit,
            ),
            ("workers", snapshot.workers, snapshot.worker_limit),
            ("bytes", snapshot.bytes, snapshot.byte_limit),
        ] {
            for (dimension, value) in [("used", used), ("limit", limit)] {
                assert!(prometheus.contains(&format!("novarocks_backend_task_preparation{{dimension=\"{dimension}\",resource=\"{resource}\"}} {value}")), "{prometheus}");
                assert!(
                    rows.iter().any(|row| row["tags"]["metric"]
                        == "novarocks_backend_task_preparation"
                        && row["tags"]["resource"] == resource
                        && row["tags"]["dimension"] == dimension
                        && row["value"].as_f64() == Some(value as f64)),
                    "{json}"
                );
            }
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    fn preparation_busy_scrape_omits_stale_values_without_blocking_control_metrics() {
        let owner = preparation_owner();
        let backend = Arc::new(
            BackendMetricsRegistry::new()
                .unwrap()
                .with_task_preparation(Arc::clone(&owner))
                .with_worker_registry_lock(owner.registry_lock_observation()),
        );
        assert!(
            render_metrics(&backend)
                .unwrap()
                .contains("novarocks_backend_task_preparation_snapshot_available 1")
        );
        backend
            .task_preparation_gauges
            .with_label_values(&["positions", "used"])
            .set(123);
        let _ = BACKEND_NATIVE_INGRESS_SLOTS.with_label_values(&["control", "running", "used"]);
        native_control_queue_wait("started", std::time::Duration::from_millis(2));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            owner.with_registry_lock_for_test(|| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("actual registry lock acquired");
        let metrics = Arc::clone(&backend);
        let (rendered_tx, rendered_rx) = std::sync::mpsc::channel();
        let scrape = std::thread::spawn(move || {
            rendered_tx
                .send((render_metrics(&metrics), render_metrics_json(&metrics)))
                .unwrap();
        });
        let rendered = rendered_rx.recv_timeout(std::time::Duration::from_secs(5));
        // Always release the real lock before asserting the nonblocking oracle.
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        scrape.join().unwrap();
        let (prometheus, json) =
            rendered.expect("both metrics formats must finish while the registry lock is held");
        let prometheus = prometheus.unwrap();
        let json = json.unwrap();
        assert_preparation_unavailable(&prometheus, &json);
        assert!(prometheus.contains("novarocks_backend_native_ingress_slots{class=\"control\",dimension=\"used\",phase=\"running\"}"));
        assert!(prometheus.contains(
            "novarocks_backend_native_control_queue_wait_seconds_count{outcome=\"started\"}"
        ));
        assert!(prometheus.contains("novarocks_backend_worker_registry_lock_observation"));
        let rows: serde_json::Value = serde_json::from_str(&json).unwrap();
        let rows = rows.as_array().unwrap();
        for name in [
            "novarocks_backend_native_ingress_slots",
            "novarocks_backend_native_control_queue_wait_seconds_count",
            "novarocks_backend_worker_registry_lock_observation",
        ] {
            assert!(
                rows.iter().any(|row| row["tags"]["metric"] == name),
                "{json}"
            );
        }
        assert!(
            render_metrics(&backend)
                .unwrap()
                .contains("novarocks_backend_task_preparation_snapshot_available 1")
        );
        assert!(
            render_metrics_json(&backend)
                .unwrap()
                .contains("novarocks_backend_task_preparation\"")
        );
        assert_eq!(
            backend
                .task_preparation_gauges
                .with_label_values(&["positions", "used"])
                .get(),
            0
        );
    }

    #[cfg(debug_assertions)]
    #[test]
    fn preparation_poison_is_an_error_in_both_formats() {
        let owner = preparation_owner();
        let backend = BackendMetricsRegistry::new()
            .unwrap()
            .with_task_preparation(Arc::clone(&owner));
        let poison = std::thread::spawn(move || {
            owner.with_registry_lock_for_test(|| panic!("poison actual registry mutex"))
        });
        assert!(poison.join().is_err());
        for result in [render_metrics(&backend), render_metrics_json(&backend)] {
            let error = result.expect_err("poison must not become busy or zero usage");
            assert!(
                error.contains("sample Worker preparation charge ledger"),
                "{error}"
            );
            assert!(error.to_ascii_lowercase().contains("poison"), "{error}");
        }
    }

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

    fn process_memory(
        allocator: &'static str,
        allocator_settings: Vec<(&'static str, i64)>,
        sample: impl Fn() -> PhysicalMemoryReading + Send + Sync + 'static,
    ) -> ProcessMemoryObservation {
        ProcessMemoryObservation {
            allocator,
            allocator_settings,
            visible_memory: Some((16 << 30, "cgroup")),
            sample: Arc::new(move || {
                let mut counted = AttributionSnapshot::default();
                counted.process.small.live_bytes = 1024;
                counted.process.tagged.live_bytes = 3072;
                counted.process.total.live_bytes = 4096;
                (counted, sample())
            }),
        }
    }

    fn attribution_fixture() -> AttributionSnapshot {
        use novarocks_memory::attribution::readout::ClassFacts;
        let mut sample = AttributionSnapshot::default();
        sample.process.small.live_bytes = 80;
        sample.process.small.allocated_total_bytes = 100;
        sample.process.small.deallocated_total_bytes = 20;
        sample.process.small.allocations = 3;
        sample.process.small.reallocations = 4;
        sample.process.small.failures = 5;
        sample.process.small_deallocations = 2;
        sample.process.tagged.live_bytes = 1024;
        sample.process.tagged.allocated_total_bytes = 2048;
        sample.process.tagged.deallocated_total_bytes = 1024;
        sample.process.tagged.allocations = 7;
        sample.process.tagged.reallocations = 8;
        sample.process.tagged.failures = 9;
        sample.process.tagged_deallocations = 6;
        sample.process.total_deallocations = 8;
        sample.classified = [
            ClassFacts {
                tagged_bytes: 512,
                r1_small_bytes: 20,
            },
            ClassFacts {
                tagged_bytes: 200,
                r1_small_bytes: 15,
            },
            ClassFacts {
                tagged_bytes: 256,
                r1_small_bytes: 5,
            },
        ];
        sample.unattributed_tagged_bytes = 56;
        sample.ledger_blind_spot_bytes = 40;
        sample.tagged_reconciliation_bytes = 0;
        sample.records = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [16, 0, 0]];
        sample.record_capacity = 262144;
        sample.record_high_water = 22;
        sample.draining_records = 3;
        sample.record_segment_requested_bytes = 262144;
        sample.observation_metadata_bytes = Some(2048);
        sample.faults.binding_failures = 1;
        sample.faults.record_exhaustions = 2;
        sample.faults.orphan_events = 3;
        sample.faults.residual_growth_events = 4;
        sample.faults.scope_refusals = 5;
        sample.faults.reclaim_nonzero_events = 6;
        sample.faults.generation_exhaustions = 7;
        sample.faults.pinned_slots = 2;
        sample.slot_balance_estimate_excluding_in_flight_bytes = 2 * sample.batch_threshold_bytes;
        sample.sampled_at_unix_millis = Some(1_700_000_000_000);
        sample.sequence_sum = 42;
        sample
    }

    fn attribution_backend(
        sample: impl Fn() -> AttributionSnapshot + Send + Sync + 'static,
    ) -> BackendMetricsRegistry {
        BackendMetricsRegistry::new()
            .unwrap()
            .with_process_memory(ProcessMemoryObservation {
                allocator: "system",
                allocator_settings: Vec::new(),
                visible_memory: None,
                sample: Arc::new(move || (sample(), PhysicalMemoryReading::default())),
            })
            .unwrap()
    }

    #[test]
    fn attribution_families_export_all_bounded_dimensions_in_text_and_json() {
        let backend = attribution_backend(attribution_fixture);
        let rendered = render_metrics(&backend).unwrap();
        let json: serde_json::Value =
            serde_json::from_str(&render_metrics_json(&backend).unwrap()).unwrap();
        for (suffix, labels, value) in [
            ("process_counted_live_bytes", vec![("band", "small")], 80),
            ("process_counted_live_bytes", vec![("band", "tagged")], 1024),
            (
                "process_counted_operations_total",
                vec![("band", "small"), ("kind", "dealloc")],
                2,
            ),
            (
                "process_counted_operations_total",
                vec![("band", "tagged"), ("kind", "failure")],
                9,
            ),
            (
                "process_counted_requested_bytes_total",
                vec![("band", "small"), ("flow", "deallocated")],
                20,
            ),
            (
                "memory_attributed_bytes",
                vec![("band", "tagged"), ("class", "query")],
                512,
            ),
            (
                "memory_attributed_bytes",
                vec![("band", "r1_small"), ("class", "residual")],
                15,
            ),
            (
                "memory_attributed_bytes",
                vec![("band", "tagged"), ("class", "service")],
                256,
            ),
            ("memory_unattributed_bytes", vec![], 56),
            ("memory_ledger_blind_spot_bytes", vec![], 40),
            ("memory_attribution_reconcile_bytes", vec![], 0),
            (
                "memory_lane_records",
                vec![("class", "query"), ("production", "sealed")],
                2,
            ),
            (
                "memory_lane_records",
                vec![("class", "residual"), ("production", "stopped")],
                6,
            ),
            ("memory_lane_record_capacity", vec![], 262144),
            ("memory_lane_record_high_water", vec![], 22),
            ("memory_lane_records_draining", vec![], 3),
            ("memory_lane_record_segment_requested_bytes", vec![], 262144),
            ("memory_observation_metadata_bytes", vec![], 2048),
            (
                "memory_attribution_faults_total",
                vec![("kind", "binding_failure")],
                1,
            ),
            (
                "memory_attribution_faults_total",
                vec![("kind", "generation_exhaustion")],
                7,
            ),
            ("memory_batch_threshold_bytes", vec![], 1048576),
            ("memory_batch_pinned_slots", vec![], 2),
            ("memory_batch_slot_balance_estimate_bytes", vec![], 2097152),
            (
                "memory_attribution_sample_unixtime_seconds",
                vec![],
                1700000000,
            ),
            ("memory_attribution_sequence_sum", vec![], 42),
        ] {
            let name = format!("novarocks_backend_{suffix}");
            let text_labels = if labels.is_empty() {
                String::new()
            } else {
                format!(
                    "{{{}}}",
                    labels
                        .iter()
                        .map(|(key, value)| format!("{key}=\"{value}\""))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            };
            let line = format!("{name}{text_labels} {value}");
            assert!(rendered.contains(&line), "missing {line}\n{rendered}");
            assert!(
                json.as_array()
                    .unwrap()
                    .iter()
                    .any(|row| row["tags"]["metric"] == name
                        && labels
                            .iter()
                            .all(|(key, value)| row["tags"][*key] == *value)
                        && row["value"].as_f64() == Some(value as f64)),
                "missing {line} in {json}"
            );
        }
        let rows = json.as_array().unwrap();
        for (family, count) in [
            ("memory_attributed_bytes", 6),
            ("memory_lane_records", 12),
            ("memory_attribution_faults_total", 7),
            ("process_counted_operations_total", 8),
            ("process_counted_requested_bytes_total", 4),
        ] {
            let name = format!("novarocks_backend_{family}");
            assert_eq!(
                rows.iter()
                    .filter(|row| row["tags"]["metric"] == name)
                    .count(),
                count
            );
        }
        assert!(!rendered.contains("novarocks_backend_process_counted_live_bytes  "));
        assert!(!rendered.contains("novarocks_backend_process_counted_live_bytes 80"));
        assert!(rendered.contains("excluding in-flight allocations"));
        assert!(rendered.contains("not an instantaneous or concurrent mathematical upper bound"));
    }

    #[test]
    fn attribution_refresh_preserves_signed_samples_removes_unknowns_and_does_not_accumulate_reads()
    {
        let sample = Arc::new(Mutex::new(attribution_fixture()));
        let captured = Arc::clone(&sample);
        let backend = attribution_backend(move || *captured.lock().unwrap());
        let initial = render_metrics(&backend).unwrap();
        assert!(initial.contains("novarocks_backend_memory_observation_metadata_bytes 2048"));
        {
            let mut current = sample.lock().unwrap();
            current.classified[0].tagged_bytes = -5;
            current.ledger_blind_spot_bytes = -7;
            current.tagged_reconciliation_bytes = 8;
            current.observation_metadata_bytes = None;
            current.sampled_at_unix_millis = None;
        }
        for _ in 0..2 {
            let rendered = render_metrics(&backend).unwrap();
            assert!(rendered.contains(
                "novarocks_backend_memory_attributed_bytes{band=\"tagged\",class=\"query\"} -5"
            ));
            assert!(rendered.contains("novarocks_backend_memory_ledger_blind_spot_bytes -7"));
            assert!(rendered.contains("novarocks_backend_memory_attribution_reconcile_bytes 8"));
            assert!(rendered.contains("novarocks_backend_process_counted_operations_total{band=\"small\",kind=\"dealloc\"} 2"));
            assert!(!rendered.contains("novarocks_backend_memory_observation_metadata_bytes "));
            assert!(
                !rendered.contains("novarocks_backend_memory_attribution_sample_unixtime_seconds ")
            );
        }
        let rows: serde_json::Value =
            serde_json::from_str(&render_metrics_json(&backend).unwrap()).unwrap();
        assert!(!rows.as_array().unwrap().iter().any(|row| matches!(
            row["tags"]["metric"].as_str(),
            Some(
                "novarocks_backend_memory_observation_metadata_bytes"
                    | "novarocks_backend_memory_attribution_sample_unixtime_seconds"
            )
        )));
        assert!(
            rows.as_array()
                .unwrap()
                .iter()
                .any(|row| row["tags"]["metric"]
                    == "novarocks_backend_memory_ledger_blind_spot_bytes"
                    && row["value"].as_f64() == Some(-7.0))
        );
    }

    #[test]
    fn attribution_registry_ownership_and_signed_range_are_preserved() {
        let backend = attribution_backend(attribution_fixture);
        assert!(
            render_metrics(&backend)
                .unwrap()
                .contains("novarocks_backend_memory_unattributed_bytes 56")
        );
        let empty = BackendMetricsRegistry::new().unwrap();
        assert!(
            !render_metrics(&empty)
                .unwrap()
                .contains("novarocks_backend_memory_attributed_bytes")
        );
        assert!(
            !render_metrics_json(&empty)
                .unwrap()
                .contains("novarocks_backend_process_counted_live_bytes")
        );
        assert_eq!(signed_gauge_value(i128::MIN), i64::MIN);
        assert_eq!(signed_gauge_value(-1), -1);
        assert_eq!(signed_gauge_value(i128::MAX), i64::MAX);
    }

    #[test]
    fn process_memory_readings_are_rendered_in_both_formats() {
        let reading = PhysicalMemoryReading {
            cgroup_anonymous_bytes: Some(3000),
            process_resident_bytes: None,
            allocator_internals: Some(novarocks_memory::observe::AllocatorInternalsReading {
                allocated_bytes: 100,
                active_bytes: 120,
                resident_bytes: 150,
            }),
        };
        let backend = BackendMetricsRegistry::new()
            .expect("construct Backend registry")
            .with_process_memory(process_memory(
                "jemalloc",
                vec![("background_thread", 1), ("dirty_decay_ms", 10_000)],
                move || reading,
            ))
            .expect("register process memory");
        let rendered = render_metrics(&backend).expect("render Backend metrics");
        for line in [
            "novarocks_backend_process_visible_memory_bytes{bound=\"cgroup\"} 17179869184",
            "novarocks_backend_process_allocator_info{allocator=\"jemalloc\"} 1",
            "novarocks_backend_process_allocator_setting{setting=\"background_thread\"} 1",
            "novarocks_backend_process_allocator_setting{setting=\"dirty_decay_ms\"} 10000",
            "novarocks_backend_process_physical_memory_bytes{source=\"cgroup_anonymous\"} 3000",
            "novarocks_backend_process_allocator_memory_bytes{statistic=\"allocated\"} 100",
            "novarocks_backend_process_allocator_memory_bytes{statistic=\"active\"} 120",
            "novarocks_backend_process_allocator_memory_bytes{statistic=\"resident\"} 150",
            "novarocks_backend_process_counted_live_bytes{band=\"small\"} 1024",
            "novarocks_backend_process_counted_live_bytes{band=\"tagged\"} 3072",
        ] {
            assert!(rendered.contains(line), "missing {line}\n{rendered}");
        }
        assert!(
            !rendered.contains("source=\"process_resident\""),
            "an unread source must be absent, not zero\n{rendered}"
        );
        let json = render_metrics_json(&backend).expect("render Backend JSON");
        let rows: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        assert!(rows.as_array().is_some_and(|rows| rows.iter().any(|row| {
            row["tags"]["metric"] == "novarocks_backend_process_allocator_memory_bytes"
                && row["tags"]["statistic"] == "resident"
                && row["value"].as_f64() == Some(150.0)
        })));
    }

    #[test]
    fn a_system_allocator_exports_no_allocator_statistics_or_settings() {
        let backend = BackendMetricsRegistry::new()
            .expect("construct Backend registry")
            .with_process_memory(process_memory("system", Vec::new(), || {
                PhysicalMemoryReading {
                    process_resident_bytes: Some(5000),
                    ..PhysicalMemoryReading::default()
                }
            }))
            .expect("register process memory");
        let rendered = render_metrics(&backend).expect("render Backend metrics");
        assert!(
            rendered.contains("novarocks_backend_process_allocator_info{allocator=\"system\"} 1")
        );
        assert!(rendered.contains(
            "novarocks_backend_process_physical_memory_bytes{source=\"process_resident\"} 5000"
        ));
        let json = render_metrics_json(&backend).unwrap();
        assert!(!json.contains("novarocks_backend_process_allocator_memory_bytes"));
        assert!(!json.contains("novarocks_backend_process_allocator_setting"));
        assert!(
            !rendered.contains("novarocks_backend_process_allocator_memory_bytes{"),
            "{rendered}"
        );
        assert!(
            !rendered.contains("novarocks_backend_process_allocator_setting{"),
            "{rendered}"
        );
    }

    #[test]
    fn a_reading_that_disappears_is_removed_not_left_stale() {
        let readable = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let observed = Arc::clone(&readable);
        let backend = BackendMetricsRegistry::new()
            .expect("construct Backend registry")
            .with_process_memory(process_memory("system", Vec::new(), move || {
                PhysicalMemoryReading {
                    process_resident_bytes: observed.load(AtomicOrdering::Relaxed).then_some(5000),
                    ..PhysicalMemoryReading::default()
                }
            }))
            .expect("register process memory");
        let series = "novarocks_backend_process_physical_memory_bytes{source=\"process_resident\"}";
        assert!(render_metrics(&backend).expect("render").contains(series));
        readable.store(false, AtomicOrdering::Relaxed);
        assert!(!render_metrics(&backend).expect("render").contains(series));
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
