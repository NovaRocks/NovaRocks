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

//! Original task transport counters and collector registration.

use super::*;

pub(super) const REFUSAL_UNKNOWN_BACKEND: &str = "unknown_backend";
pub(super) const REFUSAL_PROCESS_MISMATCH: &str = "process_mismatch";
pub(super) const REFUSAL_UNENCODABLE: &str = "unencodable";
pub(super) const REFUSAL_OVER_BUDGET: &str = "over_budget";
pub(super) const REFUSAL_UNUSABLE_RESPONSE: &str = "unusable_response";
pub(super) const REFUSAL_UNUSABLE_ACK: &str = "unusable_ack";

pub(super) const fn lane_name(lane: DispatchLane) -> &'static str {
    match lane {
        DispatchLane::Create => "create",
        DispatchLane::Update => "update",
        DispatchLane::Lifecycle => "lifecycle",
        DispatchLane::Control => "control",
    }
}

/// The machine-readable category name of one outcome.
///
/// This is the same closed category set the protocol classifies retry and
/// failure from, so an operator reading the metric and an owner reading the
/// receipt share one vocabulary.
pub(super) const fn outcome_name(outcome: OperationOutcome) -> &'static str {
    match outcome {
        OperationOutcome::OwnerFailed => "owner_failed",
        OperationOutcome::Accepted => "accepted",
        OperationOutcome::Idempotent => "idempotent",
        OperationOutcome::OperationTimedOut => "operation_timed_out",
        OperationOutcome::IdentityMismatch => "identity_mismatch",
        OperationOutcome::CompatibilityMismatch => "compatibility_mismatch",
        OperationOutcome::ParticipationNotEstablished => "context_not_established",
        OperationOutcome::ParticipationConflict => "context_conflict",
        OperationOutcome::DomainConflict => "domain_conflict",
        OperationOutcome::LeaseExpired => "lease_expired",
        OperationOutcome::ReleaseNotReady => "release_not_ready",
        OperationOutcome::ParticipationTerminalReceipt => "context_terminal_receipt",
        OperationOutcome::InvalidStateOrRequest => "invalid_state_or_request",
        OperationOutcome::TerminalRejected => "terminal_rejected",
        OperationOutcome::Gone => "gone",
        OperationOutcome::ResourceExhausted => "resource_exhausted",
        OperationOutcome::NotReady => "not_ready",
        OperationOutcome::PreparationBusy => "preparation_busy",
        OperationOutcome::AdmissionTicketStillActive => "admission_ticket_still_active",
    }
}

pub(super) static TASK_DISPATCH_LANE_DEPTH: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_task_dispatch_lane_depth",
            "Task operations queued or in flight on one frontend dispatch lane.",
        ),
        &["lane", "phase"],
    )
    .expect("register novarocks_task_dispatch_lane_depth")
});

pub(super) static TASK_OPERATION_BATCH_ITEMS: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "novarocks_task_operation_batch_items",
            "Operations carried by one ApplyTaskOperations request.",
        )
        .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0]),
        &["lane"],
    )
    .expect("register novarocks_task_operation_batch_items")
});

pub(super) static TASK_OPERATION_BATCH_BYTES: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "novarocks_task_operation_batch_bytes",
            "Encoded size of one ApplyTaskOperations request in bytes.",
        )
        .buckets(vec![
            1024.0,
            64.0 * 1024.0,
            1024.0 * 1024.0,
            8.0 * 1024.0 * 1024.0,
            48.0 * 1024.0 * 1024.0,
        ]),
        &["lane"],
    )
    .expect("register novarocks_task_operation_batch_bytes")
});

pub(super) static TASK_OPERATION_RECEIPTS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_task_operation_receipt_total",
            "Settled task operations by operation kind and outcome category.",
        ),
        &["kind", "outcome"],
    )
    .expect("register novarocks_task_operation_receipt_total")
});

pub(super) static TASK_OPERATION_REFUSALS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_task_operation_refused_total",
            "Task operations the frontend refused without a usable backend receipt.",
        ),
        &["reason"],
    )
    .expect("register novarocks_task_operation_refused_total")
});

pub(super) static TASK_STATUS_SUBSCRIPTION_STATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_task_status_subscription_state_total",
            "Observed states of the frontend task status subscriptions.",
        ),
        &["state"],
    )
    .expect("register novarocks_task_status_subscription_state_total")
});

pub(super) static TASK_STATUS_RESUBSCRIPTIONS: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(Opts::new(
        "novarocks_task_status_resubscribe_total",
        "Task status subscriptions reopened from their per-task cursors.",
    ))
    .expect("register novarocks_task_status_resubscribe_total")
});

pub(super) static PARTICIPATION_LEASE_RENEWALS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_participation_lease_renewal_total",
            "Query execution lease renewals settled by the frontend, by outcome category.",
        ),
        &["outcome"],
    )
    .expect("register novarocks_participation_lease_renewal_total")
});

pub(super) static TASK_ATTEMPT_PUMPS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_task_attempt_pump_installed_total",
            "Per-attempt owners handed to a task runner, by owner.",
        ),
        &["pump"],
    )
    .expect("register novarocks_task_attempt_pump_installed_total")
});

/// Registers every task transport collector.
///
/// The frontend management host owns the role-local registry; this is the one
/// entry point it needs.
pub(crate) fn register_metric_collectors(registry: &Registry) -> Result<(), String> {
    for collector in [
        Box::new(TASK_DISPATCH_LANE_DEPTH.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(TASK_OPERATION_BATCH_ITEMS.clone()),
        Box::new(TASK_OPERATION_BATCH_BYTES.clone()),
        Box::new(TASK_OPERATION_RECEIPTS.clone()),
        Box::new(TASK_OPERATION_REFUSALS.clone()),
        Box::new(TASK_STATUS_SUBSCRIPTION_STATE.clone()),
        Box::new(TASK_STATUS_RESUBSCRIPTIONS.clone()),
        Box::new(PARTICIPATION_LEASE_RENEWALS.clone()),
        Box::new(TASK_ATTEMPT_PUMPS.clone()),
    ] {
        registry
            .register(collector)
            .map_err(|error| format!("register task transport metrics failed: {error}"))?;
    }
    Ok(())
}

/// Records that one attempt-local owner was handed to a task runner.
///
/// This is the supply observable of the per-turn owners. It counts installs,
/// not constructions: an owner that is built and then dropped -- which is
/// exactly how the dynamic filter and credential loops were lost -- never
/// reaches this.
pub(crate) fn observe_attempt_pump_installed(pump: &'static str) {
    TASK_ATTEMPT_PUMPS.with_label_values(&[pump]).inc();
}

/// The installs recorded for one owner so far.
#[cfg(test)]
pub(crate) fn installed_attempt_pumps(pump: &str) -> u64 {
    TASK_ATTEMPT_PUMPS.with_label_values(&[pump]).get()
}

/// Publishes one dispatch lane's depth.
///
/// The queue belongs to the dispatcher, not to this module, so the owner that
/// pumps it reports both numbers together and there is exactly one writer.
pub(crate) fn observe_dispatch_lane(lane: DispatchLane, queued: usize, in_flight: usize) {
    TASK_DISPATCH_LANE_DEPTH
        .with_label_values(&[lane_name(lane), "queued"])
        .set(queued as i64);
    TASK_DISPATCH_LANE_DEPTH
        .with_label_values(&[lane_name(lane), "in_flight"])
        .set(in_flight as i64);
}

pub(super) fn observe_batch(lane: DispatchLane, items: usize, encoded_bytes: usize) {
    TASK_OPERATION_BATCH_ITEMS
        .with_label_values(&[lane_name(lane)])
        .observe(items as f64);
    TASK_OPERATION_BATCH_BYTES
        .with_label_values(&[lane_name(lane)])
        .observe(encoded_bytes as f64);
}

/// Counts one settled operation, and one lease renewal when that is what it
/// was.
///
/// The renewal counter only observes. There is deliberately no path from a
/// transport observation back to a lease: a renewal is minted by the query
/// participation owner from its own clock, so transport health can never extend one.
pub(super) fn observe_settled(kind: OperationKind, lease_renewal: bool, outcome: OperationOutcome) {
    TASK_OPERATION_RECEIPTS
        .with_label_values(&[kind.as_str(), outcome_name(outcome)])
        .inc();
    if lease_renewal {
        PARTICIPATION_LEASE_RENEWALS
            .with_label_values(&[outcome_name(outcome)])
            .inc();
    }
}

pub(super) fn observe_dispatch_result(
    kind: OperationKind,
    lease_renewal: bool,
    result: OperationDispatchResult,
) {
    let name = match result {
        OperationDispatchResult::WorkerReceipt(receipt) => outcome_name(receipt.outcome()),
        OperationDispatchResult::IngressRejected(kind) => match kind {
            crate::task_execution::operation::IngressRejection::WaitingCapacity => {
                "ingress_waiting_capacity"
            }
            crate::task_execution::operation::IngressRejection::BodyLimit => "ingress_body_limit",
            crate::task_execution::operation::IngressRejection::UnclassifiedCapacity => {
                "ingress_capacity_unclassified"
            }
        },
        OperationDispatchResult::NonWorkerRejected => "non_worker_rejected",
        OperationDispatchResult::TransportUnknown => "transport_unknown",
    };
    TASK_OPERATION_RECEIPTS
        .with_label_values(&[kind.as_str(), name])
        .inc();
    if lease_renewal {
        PARTICIPATION_LEASE_RENEWALS
            .with_label_values(&[name])
            .inc();
    }
}

pub(super) fn observe_refusal(reason: &str, operations: usize) {
    TASK_OPERATION_REFUSALS
        .with_label_values(&[reason])
        .inc_by(operations as u64);
}

pub(super) fn observe_subscription_state(state: SubscriptionState) {
    TASK_STATUS_SUBSCRIPTION_STATE
        .with_label_values(&[state.as_str()])
        .inc();
}

pub(super) fn observe_resubscribe() {
    TASK_STATUS_RESUBSCRIPTIONS.inc();
}
