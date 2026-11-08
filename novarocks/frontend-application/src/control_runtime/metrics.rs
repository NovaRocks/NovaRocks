// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Non-authorizing observations from original owners. A scrape never rebuilds
//! capacity/protocol state from bounded history, drives work, or substitutes an
//! older/zero sample for Busy. Wait counters are the original runtime counters;
//! pending inventory comes directly from original batches and HostMachine.

use super::host::HostRegistry;
use crate::native::transport_capacity::NativeTransportCapacity;
use novarocks_control_runtime::{
    actor_turn::Clock,
    observation::{CapacityRefusalKind, RuntimeObservation, RuntimeObservationSnapshot},
};
use novarocks_control_runtime_core::identity::Tick;
use prometheus::{IntGaugeVec, Opts, Registry};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

pub(crate) struct PendingEffectObservation {
    pub(crate) pending: u64,
    pub(crate) claimed_not_handed_off: u64,
    pub(crate) unindexed_inputs: u64,
}
#[derive(Clone, Copy)]
pub(crate) struct HostObservation {
    pub(crate) turns: u64,
    pub(crate) units: u64,
    pub(crate) handoffs: u64,
    pub(crate) elapsed_ns: u64,
    pub(crate) maximum_elapsed_ns: u64,
}
/// Only cumulative metadata-turn observations live here. They are never read
/// by admission, dispatch, shutdown readiness, or physical-exit adjudication.
pub(crate) struct FrontendHostObservations {
    clock: Arc<dyn Clock>,
    valid: AtomicBool,
    turns: AtomicU64,
    units: AtomicU64,
    handoffs: AtomicU64,
    elapsed: AtomicU64,
    maximum_elapsed: AtomicU64,
}
impl FrontendHostObservations {
    pub(crate) fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            valid: AtomicBool::new(true),
            turns: AtomicU64::new(0),
            units: AtomicU64::new(0),
            handoffs: AtomicU64::new(0),
            elapsed: AtomicU64::new(0),
            maximum_elapsed: AtomicU64::new(0),
        }
    }
    pub(crate) fn now(&self) -> Tick {
        self.clock.now()
    }
    fn add(&self, counter: &AtomicU64, amount: u64) {
        if counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |previous| {
                previous.checked_add(amount)
            })
            .is_err()
        {
            self.valid.store(false, Ordering::Release);
        }
    }
    pub(crate) fn host_turn(&self, began: Tick, units: usize, handoffs: usize) {
        let Some(elapsed) = self.clock.now().0.checked_sub(began.0) else {
            self.valid.store(false, Ordering::Release);
            return;
        };
        let (Ok(units), Ok(handoffs)) = (u64::try_from(units), u64::try_from(handoffs)) else {
            self.valid.store(false, Ordering::Release);
            return;
        };
        self.add(&self.turns, 1);
        self.add(&self.units, units);
        self.add(&self.handoffs, handoffs);
        self.add(&self.elapsed, elapsed);
        self.maximum_elapsed.fetch_max(elapsed, Ordering::Relaxed);
    }
    fn sample(&self) -> Option<HostObservation> {
        self.valid.load(Ordering::Acquire).then(|| HostObservation {
            turns: self.turns.load(Ordering::Relaxed),
            units: self.units.load(Ordering::Relaxed),
            handoffs: self.handoffs.load(Ordering::Relaxed),
            elapsed_ns: self.elapsed.load(Ordering::Relaxed),
            maximum_elapsed_ns: self.maximum_elapsed.load(Ordering::Relaxed),
        })
    }
}
pub(crate) struct FrontendRuntimeMetrics {
    original: RuntimeObservation,
    host: Arc<FrontendHostObservations>,
    owner: std::sync::Weak<HostRegistry>,
    layout: NativeTransportCapacity,
    inputs: IntGaugeVec,
    ledger: IntGaugeVec,
    refusals: IntGaugeVec,
    process: IntGaugeVec,
    keyed: IntGaugeVec,
    waits: IntGaugeVec,
    pending: IntGaugeVec,
    host_turns: IntGaugeVec,
    available: IntGaugeVec,
}
fn gauge(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<IntGaugeVec, String> {
    let gauge = IntGaugeVec::new(Opts::new(name, help), labels)
        .map_err(|error| format!("construct frontend control-runtime metric: {error}"))?;
    registry
        .register(Box::new(gauge.clone()))
        .map_err(|error| format!("register frontend control-runtime metric: {error}"))?;
    Ok(gauge)
}
fn metric_values<const N: usize>(values: [u64; N]) -> Option<[i64; N]> {
    let mut result = [0; N];
    for (index, value) in values.into_iter().enumerate() {
        result[index] = i64::try_from(value).ok()?;
    }
    Some(result)
}
fn counters_representable(sample: &RuntimeObservationSnapshot) -> bool {
    let mut values = vec![
        sample.ledger.lock_attempts,
        sample.ledger.contended_locks,
        sample.ledger.poisoned_locks,
        sample.ledger.unavailable_try_locks,
        sample.capacity_operations,
        sample.capacity_wait_registrations,
        sample.capacity_wait_domains_registered,
        sample.capacity_wait_deliveries,
        sample.scope_failures,
        sample.invalid_clock_observations,
    ];
    values.extend(sample.capacity_refusals);
    for input in &sample.inputs {
        values.extend([
            input.steps,
            input.input_credits_held,
            input.charged_work,
            input.maximum_step_work,
            input.panicked_steps,
        ]);
        for duration in [input.handoff_to_step, input.step_elapsed]
            .into_iter()
            .flatten()
        {
            values.extend([duration.samples, duration.total_ns, duration.maximum_ns]);
        }
    }
    values.into_iter().all(|value| i64::try_from(value).is_ok())
}
impl FrontendRuntimeMetrics {
    pub(crate) fn new(
        registry: &Registry,
        original: RuntimeObservation,
        host: Arc<FrontendHostObservations>,
        owner: &Arc<HostRegistry>,
        layout: NativeTransportCapacity,
    ) -> Result<Self, String> {
        Ok(Self {
            original,
            host,
            owner: Arc::downgrade(owner),
            layout,
            inputs: gauge(
                registry,
                "novarocks_frontend_control_runtime_inputs",
                "Original input/step counters. Handoff time includes original pre-lock waiting; elapsed time is not CPU time. Missing observations grant no exit or resource proof.",
                &["class", "resource"],
            )?,
            ledger: gauge(
                registry,
                "novarocks_frontend_control_runtime_ledger",
                "Original cumulative ledger contention, capacity transactions and Scope failure observations.",
                &["resource"],
            )?,
            refusals: gauge(
                registry,
                "novarocks_frontend_control_runtime_capacity_refusals",
                "Original typed capacity refusals; ordinary exhaustion is backpressure, not an invariant failure.",
                &["reason"],
            )?,
            process: gauge(
                registry,
                "novarocks_frontend_control_runtime_transport_process",
                "Nonblocking original process dimension usage/limits; unavailable samples are omitted.",
                &["dimension", "resource"],
            )?,
            keyed: gauge(
                registry,
                "novarocks_frontend_control_runtime_transport_keyed",
                "Original live backend-key dimension usage/limits. Retired keys disappear. Samples are diagnostic intervals and never admission grants.",
                &["backend", "dimension", "resource"],
            )?,
            waits: gauge(
                registry,
                "novarocks_frontend_control_runtime_capacity_waits",
                "Original logical wait inventory, registered dimension domains and consumed release hints. A hint never proves capacity is available.",
                &["resource"],
            )?,
            pending: gauge(
                registry,
                "novarocks_frontend_control_runtime_pending_effects",
                "Original pending/claimed handoff inventory and unindexed input gap, obtained from canonical runtime routing observations.",
                &["state"],
            )?,
            host_turns: gauge(
                registry,
                "novarocks_frontend_control_runtime_host_turns",
                "Cumulative finite metadata host-turn units/handoffs and elapsed observations. Handoff completion is not physical exit.",
                &["resource"],
            )?,
            available: gauge(
                registry,
                "novarocks_frontend_control_runtime_observation_available",
                "Whether this scrape obtained representable original observations. Unavailable source families are omitted, never filled with cached/guessed zero.",
                &["source"],
            )?,
        })
    }
    pub(crate) fn sample(&self) {
        self.available.reset();
        self.inputs.reset();
        self.ledger.reset();
        self.refusals.reset();
        self.process.reset();
        self.keyed.reset();
        self.waits.reset();
        self.pending.reset();
        self.host_turns.reset();
        let original = self.original.snapshot().filter(counters_representable);
        self.available
            .with_label_values(&["counters"])
            .set(i64::from(original.is_some()));
        if let Some(original) = original {
            for input in original.inputs {
                let class = format!("{:?}", input.class);
                for (resource, value) in [
                    ("steps", input.steps),
                    ("credits_held", input.input_credits_held),
                    ("charged_work", input.charged_work),
                    ("max_step_work", input.maximum_step_work),
                    ("panicked_steps", input.panicked_steps),
                ] {
                    if let Ok(value) = i64::try_from(value) {
                        self.inputs
                            .with_label_values(&[&class, resource])
                            .set(value);
                    }
                }
                for (kind, duration) in [
                    ("handoff_to_step", input.handoff_to_step),
                    ("step_elapsed", input.step_elapsed),
                ] {
                    if let Some(duration) = duration
                        && let Some([samples, total, maximum]) = metric_values([
                            duration.samples,
                            duration.total_ns,
                            duration.maximum_ns,
                        ])
                    {
                        self.inputs
                            .with_label_values(&[&class, &format!("{kind}_samples")])
                            .set(samples);
                        self.inputs
                            .with_label_values(&[&class, &format!("{kind}_total_ns")])
                            .set(total);
                        self.inputs
                            .with_label_values(&[&class, &format!("{kind}_max_ns")])
                            .set(maximum);
                    }
                }
            }
            for (resource, value) in [
                ("lock_attempts", original.ledger.lock_attempts),
                ("contended_locks", original.ledger.contended_locks),
                ("poisoned_locks", original.ledger.poisoned_locks),
                (
                    "unavailable_try_locks",
                    original.ledger.unavailable_try_locks,
                ),
                ("capacity_operations", original.capacity_operations),
                ("scope_failures", original.scope_failures),
                (
                    "invalid_clock_observations",
                    original.invalid_clock_observations,
                ),
            ] {
                if let Ok(value) = i64::try_from(value) {
                    self.ledger.with_label_values(&[resource]).set(value);
                }
            }
            for (kind, value) in CapacityRefusalKind::ALL
                .into_iter()
                .zip(original.capacity_refusals)
            {
                if let Ok(value) = i64::try_from(value) {
                    self.refusals
                        .with_label_values(&[&format!("{kind:?}")])
                        .set(value);
                }
            }
        }
        let Some(owner) = self.owner.upgrade() else {
            for source in ["transport_process", "transport_keys", "waits", "effects"] {
                self.available.with_label_values(&[source]).set(0);
            }
            return;
        };
        let (ledger, _runtime) = owner.observation_owners();
        let transport = self.layout.snapshot(ledger).ok().flatten();
        let process = transport
            .as_ref()
            .and_then(|sample| Some((metric_values(sample.used)?, metric_values(sample.limits)?)));
        self.available
            .with_label_values(&["transport_process"])
            .set(i64::from(process.is_some()));
        if let Some((used, limits)) = process {
            for (index, (used, limit)) in used.into_iter().zip(limits).enumerate() {
                let dimension = (index + 1).to_string();
                self.process
                    .with_label_values(&[&dimension, "used"])
                    .set(used);
                self.process
                    .with_label_values(&[&dimension, "limit"])
                    .set(limit);
            }
        }
        let targets = transport.and_then(|sample| {
            sample
                .targets
                .into_iter()
                .map(|target| {
                    Some((
                        target.backend,
                        metric_values(target.used)?,
                        metric_values(target.limits)?,
                    ))
                })
                .collect::<Option<Vec<_>>>()
        });
        self.available
            .with_label_values(&["transport_keys"])
            .set(i64::from(targets.is_some()));
        if let Some(targets) = targets {
            for (backend, used, limits) in targets {
                let backend = backend.to_string();
                for (index, (used, limit)) in used.into_iter().zip(limits).enumerate() {
                    let dimension = (index + 7).to_string();
                    self.keyed
                        .with_label_values(&[&backend, &dimension, "used"])
                        .set(used);
                    self.keyed
                        .with_label_values(&[&backend, &dimension, "limit"])
                        .set(limit);
                }
            }
        }
        let waits = ledger
            .try_capacity_waiter_snapshot()
            .ok()
            .flatten()
            .and_then(|sample| {
                metric_values([
                    u64::try_from(sample.logical_waiters).ok()?,
                    u64::try_from(sample.domain_registrations).ok()?,
                    u64::try_from(sample.distinct_domains).ok()?,
                ])
            });
        self.available
            .with_label_values(&["waits"])
            .set(i64::from(waits.is_some()));
        if let Some(values) = waits {
            for (resource, value) in ["logical_waits", "domain_registrations", "distinct_domains"]
                .into_iter()
                .zip(values)
            {
                self.waits.with_label_values(&[resource]).set(value);
            }
        }
        if let Some(sample) = self.original.snapshot()
            && let Some(values) = metric_values([
                sample.capacity_wait_registrations,
                sample.capacity_wait_domains_registered,
                sample.capacity_wait_deliveries,
            ])
        {
            for (resource, value) in [
                "logical_registrations_total",
                "domains_registered_total",
                "deliveries_total",
            ]
            .into_iter()
            .zip(values)
            {
                self.waits.with_label_values(&[resource]).set(value);
            }
            self.available.with_label_values(&["wait_counters"]).set(1);
        } else {
            self.available.with_label_values(&["wait_counters"]).set(0);
        }
        let effects = owner
            .try_effect_observation()
            .ok()
            .flatten()
            .and_then(|sample| {
                metric_values([
                    sample.pending,
                    sample.claimed_not_handed_off,
                    sample.unindexed_inputs,
                ])
            });
        self.available
            .with_label_values(&["effects"])
            .set(i64::from(effects.is_some()));
        if let Some(values) = effects {
            for (state, value) in ["pending", "claimed_not_handed_off", "unindexed_inputs"]
                .into_iter()
                .zip(values)
            {
                self.pending.with_label_values(&[state]).set(value);
            }
        }
        let host = self.host.sample().and_then(|sample| {
            metric_values([
                sample.turns,
                sample.units,
                sample.handoffs,
                sample.elapsed_ns,
                sample.maximum_elapsed_ns,
            ])
        });
        self.available
            .with_label_values(&["host"])
            .set(i64::from(host.is_some()));
        if let Some(values) = host {
            for (resource, value) in [
                "turns",
                "units",
                "handoffs",
                "elapsed_ns",
                "maximum_elapsed_ns",
            ]
            .into_iter()
            .zip(values)
            {
                self.host_turns.with_label_values(&[resource]).set(value);
            }
        }
    }
}
