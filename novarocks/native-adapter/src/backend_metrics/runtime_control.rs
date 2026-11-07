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

//! Project original control-runtime observations; never drive protocol work.

use novarocks_worker::{
    WorkerRuntimeInputClass, WorkerRuntimeInventory, WorkerRuntimeObservationSnapshot,
    WorkerTaskControl,
};
use prometheus::{IntGaugeVec, Opts, Registry};

pub(super) struct RuntimeControlMetrics {
    inputs: IntGaugeVec,
    ledger: IntGaugeVec,
    refusals: IntGaugeVec,
    inventory: IntGaugeVec,
    work_labels: IntGaugeVec,
    available: IntGaugeVec,
    errors: IntGaugeVec,
}
fn gauge(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<IntGaugeVec, String> {
    let value = IntGaugeVec::new(Opts::new(name, help), labels)
        .map_err(|error| format!("construct control-runtime observation: {error}"))?;
    registry
        .register(Box::new(value.clone()))
        .map_err(|error| format!("register control-runtime observation: {error}"))?;
    Ok(value)
}
impl RuntimeControlMetrics {
    pub(super) fn new(registry: &Registry) -> Result<Self, String> {
        Ok(Self {
            inputs: gauge(
                registry,
                "novarocks_backend_control_runtime_inputs",
                "Independently sampled original cumulative input/step diagnostics. Duration samples use the injected monotonic clock; handoff-to-step includes pre-lock handoff waiting, elapsed is not CPU time. Missing timing is omitted. Counts grant no resources or exit proof.",
                &["class", "resource"],
            )?,
            ledger: gauge(
                registry,
                "novarocks_backend_control_runtime_ledger",
                "Original cumulative ledger contention, capacity operations and Scope failure observations; ordinary capacity backpressure remains distinct from invariants. Inventory page progress is bounded diagnostic traversal, not protocol work.",
                &["resource"],
            )?,
            refusals: gauge(
                registry,
                "novarocks_backend_control_runtime_capacity_refusals",
                "Original cumulative capacity transaction refusals by typed category. Exhausted is ordinary backpressure; none of these counters creates a failure, retry or capacity decision.",
                &["reason"],
            )?,
            inventory: gauge(
                registry,
                "novarocks_backend_control_runtime_inventory",
                "Freshly completed bounded traversal over canonical ledger records, covering an observation interval rather than a coherent instant. Work phases are lease phases, not physical executor queues. Missing/incomplete intervals are omitted; no cached or default zero inventory is exported.",
                &["resource"],
            )?,
            work_labels: gauge(
                registry,
                "novarocks_backend_control_runtime_work_labels",
                "Original Work headers by fixed label over a freshly completed inventory interval. Live and unresolved counts are diagnostic, never exit receipts or capacity.",
                &["label", "state"],
            )?,
            available: gauge(
                registry,
                "novarocks_backend_control_runtime_observation_available",
                "Whether this scrape obtained representable original counters, a nonblocking inventory page or a freshly completed inventory interval. Unavailable observations omit their usage families.",
                &["source"],
            )?,
            errors: gauge(
                registry,
                "novarocks_backend_control_runtime_observation_errors",
                "Typed diagnostic errors in this scrape. A failed optional inventory observation omits its interval without suppressing other original metric owners.",
                &["source", "reason"],
            )?,
        })
    }
    pub(super) fn sample(&self, owner: Option<&WorkerTaskControl>) {
        self.inputs.reset();
        self.ledger.reset();
        self.refusals.reset();
        self.inventory.reset();
        self.work_labels.reset();
        self.errors.reset();
        let counters = owner
            .and_then(WorkerTaskControl::runtime_observation_snapshot)
            .and_then(counter_values);
        self.available
            .with_label_values(&["counters"])
            .set(i64::from(counters.is_some()));
        if let Some((inputs, ledger, refusals)) = counters {
            for (class, resource, value) in inputs {
                self.inputs.with_label_values(&[class, resource]).set(value);
            }
            for (resource, value) in ledger {
                self.ledger.with_label_values(&[resource]).set(value);
            }
            for (reason, value) in refusals {
                self.refusals.with_label_values(&[reason]).set(value);
            }
        }
        // One scrape visits at most this many original records, irrespective
        // of live/retained Context population. Busy never blocks on the ledger.
        let page = match owner
            .map(|owner| owner.try_runtime_inventory_page(256))
            .transpose()
        {
            Ok(page) => page.flatten(),
            Err(error) => {
                use novarocks_worker::WorkerRuntimeObservationError as Error;
                let reason = match error {
                    Error::OwnerIndexNotConfigured(_) => "owner_index_missing",
                    Error::UnknownDimension(_) => "unknown_dimension",
                    Error::Poisoned => "poisoned",
                    Error::InventoryOverflow => "inventory_overflow",
                };
                self.errors.with_label_values(&["inventory", reason]).set(1);
                None
            }
        };
        let progress = page.as_ref().and_then(|page| {
            Some((
                i64::try_from(page.visited).ok()?,
                i64::try_from(page.cumulative_visited).ok()?,
            ))
        });
        self.available
            .with_label_values(&["inventory_page"])
            .set(i64::from(progress.is_some()));
        if let Some((visited, cumulative)) = progress {
            self.ledger
                .with_label_values(&["inventory_page_records_visited"])
                .set(visited);
            self.ledger
                .with_label_values(&["inventory_interval_records_visited"])
                .set(cumulative);
        }
        let completed = page.and_then(|page| page.completed);
        if let Some(sample) = completed.as_ref() {
            for label in novarocks_worker::WORK_DIAGNOSTIC_LABELS {
                for (state, counts) in [
                    ("live", &sample.live_by_label),
                    ("unresolved", &sample.unresolved_by_label),
                ] {
                    if let Ok(count) = i64::try_from(counts.get(label).copied().unwrap_or(0)) {
                        self.work_labels
                            .with_label_values(&[label, state])
                            .set(count);
                    }
                }
            }
        }
        let inventory = completed.and_then(inventory_values);
        self.available
            .with_label_values(&["inventory_interval"])
            .set(i64::from(inventory.is_some()));
        if let Some(inventory) = inventory {
            for (resource, value) in inventory {
                self.inventory.with_label_values(&[resource]).set(value);
            }
        }
    }
}

type ScalarValues = Vec<(&'static str, i64)>;
type InputValues = Vec<(&'static str, &'static str, i64)>;
fn counter_values(
    sample: WorkerRuntimeObservationSnapshot,
) -> Option<(InputValues, ScalarValues, ScalarValues)> {
    let mut inputs = Vec::new();
    for input in sample.inputs {
        let class = match input.class {
            WorkerRuntimeInputClass::Lifecycle => "lifecycle",
            WorkerRuntimeInputClass::Control => "control",
            WorkerRuntimeInputClass::Ordinary => "ordinary",
            WorkerRuntimeInputClass::Timer => "timer",
        };
        for (resource, value) in [
            ("steps", input.steps),
            ("input_credits_held", input.input_credits_held),
            ("charged_work", input.charged_work),
            ("maximum_step_work", input.maximum_step_work),
            ("panicked_steps", input.panicked_steps),
        ] {
            inputs.push((class, resource, i64::try_from(value).ok()?));
        }
        if let Some(duration) = input.handoff_to_step {
            for (resource, value) in [
                ("handoff_to_step_samples", duration.samples),
                ("handoff_to_step_total_ns", duration.total_ns),
                ("handoff_to_step_maximum_ns", duration.maximum_ns),
            ] {
                inputs.push((class, resource, i64::try_from(value).ok()?));
            }
        }
        if let Some(duration) = input.step_elapsed {
            for (resource, value) in [
                ("step_elapsed_samples", duration.samples),
                ("step_elapsed_total_ns", duration.total_ns),
                ("step_elapsed_maximum_ns", duration.maximum_ns),
            ] {
                inputs.push((class, resource, i64::try_from(value).ok()?));
            }
        }
    }
    let ledger = [
        ("blocking_lock_attempts", sample.ledger.lock_attempts),
        ("contended_locks", sample.ledger.contended_locks),
        ("poisoned_locks", sample.ledger.poisoned_locks),
        ("unavailable_try_locks", sample.ledger.unavailable_try_locks),
        ("capacity_operations", sample.capacity_operations),
        ("scope_failures", sample.scope_failures),
        (
            "invalid_clock_observations",
            sample.invalid_clock_observations,
        ),
    ]
    .into_iter()
    .map(|(key, value)| Some((key, i64::try_from(value).ok()?)))
    .collect::<Option<Vec<_>>>()?;
    // The fixed categories follow the actual Common refusal vocabulary.
    use novarocks_worker::WorkerCapacityRefusalKind as Kind;
    let mut refusals = Vec::new();
    for kind in Kind::ALL {
        let reason = match kind {
            Kind::Exhausted => "exhausted",
            Kind::ScopeClosed => "scope_closed",
            Kind::ScopeStopped => "scope_stopped",
            Kind::ScopeFailed => "scope_failed",
            Kind::GenerationMismatch => "generation_mismatch",
            Kind::IdentityOrArithmetic => "identity_or_arithmetic",
            Kind::WrongOwnerOrState => "wrong_owner_or_state",
            Kind::OwnedStorage => "owned_storage",
        };
        refusals.push((
            reason,
            i64::try_from(sample.capacity_refusals[kind as usize]).ok()?,
        ));
    }
    Some((inputs, ledger, refusals))
}
fn inventory_values(sample: WorkerRuntimeInventory) -> Option<ScalarValues> {
    let mut values = [
        ("records_visited", sample.records_visited),
        ("scopes", sample.scopes),
        ("failed_scopes", sample.failed_scopes),
        ("stopped_scopes", sample.stopped_scopes),
        ("work_admitted", sample.admitted_work),
        ("work_queued", sample.queued_work),
        ("work_started", sample.started_work),
        ("work_body_exited", sample.body_exited_work),
        (
            "work_body_captures_exited_without_children",
            sample.fully_exited_work,
        ),
        ("bound_storage_positions", sample.bound_storage_positions),
        ("reports", sample.reports),
        ("filled_reports", sample.filled_reports),
        (
            "producer_exited_reports_awaiting_retirement",
            sample.producer_exited_reports_awaiting_retirement,
        ),
        (
            "exited_unconsumed_reports",
            sample.exited_unconsumed_reports,
        ),
        ("reservations", sample.reservations),
        ("unresolved_work", sample.unresolved_work),
        ("unused_inputs", sample.unused_owned_slots.inputs),
        ("unused_batches", sample.unused_owned_slots.batches),
        ("unused_effects", sample.unused_owned_slots.effects),
        ("unused_cuts", sample.unused_owned_slots.cuts),
        ("unused_replies", sample.unused_owned_slots.replies),
        ("unused_completions", sample.unused_owned_slots.completions),
        ("unused_captures", sample.unused_owned_slots.captures),
        (
            "unused_work_headers",
            sample.unused_owned_slots.work_headers,
        ),
    ]
    .into_iter()
    .map(|(key, value)| Some((key, i64::try_from(value).ok()?)))
    .collect::<Option<Vec<_>>>()?;
    if let Some(at) = sample.began_at {
        values.push(("interval_began_at_ns", i64::try_from(at.0).ok()?));
    }
    if let Some(at) = sample.ended_at {
        values.push(("interval_ended_at_ns", i64::try_from(at.0).ok()?));
    }
    Some(values)
}
