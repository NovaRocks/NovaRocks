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

//! Native admission acquisition for the non-actor replacement qualification.
//!
//! The logical owner supplies its original immutable admission request and
//! send generation. This client drives the original WC queue input, process
//! queue, Native action and Apply custody for one physical send. Retry and
//! qualification settlement stay with the logical owner.

use super::transport::{
    OriginalQueuedInput, PreparedSingleSend, SendKey, SingleSendContext, SingleSendError,
    SingleSendResult, send_queue_resources,
};
use crate::native::transport_capacity::{
    NativeTransportCapacity, TransportCapacityError, TransportCapacityLane,
};
use crate::task_execution::{
    intent::{AckPayload, OperationAcknowledgement, OperationIntent},
    operation::OperationDispatchResult,
};
use novarocks_control_runtime::{
    capacity_notification::CapacityRegistrant,
    obligation::{StepRegistration, WorkObligation},
    transaction::{RuntimeError, RuntimeLedger},
};
use novarocks_control_runtime_core::{
    identity::Generation,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, OperationOutcome, ParticipationAdmissionTicketReceipt,
};
use novarocks_workload_control::WorkError;
use std::{future::poll_fn, task::Poll};
use tokio::{sync::watch, time::Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplacementSendStop {
    Cancelled,
    Expired,
}

pub(crate) struct ReplacementAdmissionReceipt {
    pub(crate) dispatch: OperationDispatchResult,
    pub(crate) worker_outcome: Option<OperationOutcome>,
    pub(crate) ticket: Option<ParticipationAdmissionTicketReceipt>,
    pub(crate) governance: Option<WorkError>,
}

impl ReplacementAdmissionReceipt {
    fn observe(original: &OperationAcknowledgement) -> Self {
        Self {
            dispatch: original.dispatch_result(),
            worker_outcome: original.worker_outcome(),
            ticket: match original.payload() {
                AckPayload::AdmissionTicket(ticket) => Some(*ticket),
                _ => None,
            },
            governance: original.governance_failure().cloned(),
        }
    }
}

#[must_use = "the original qualification settles the typed admission and stop fact"]
pub(crate) struct ReplacementAdmissionSend {
    pub(crate) original: SingleSendResult<ReplacementAdmissionReceipt>,
    pub(crate) stopped: Option<ReplacementSendStop>,
}

#[derive(Debug)]
#[must_use = "retain any unresolved original Native retirement proof"]
pub(crate) enum ReplacementSendError {
    StoppedBeforeHandoff(ReplacementSendStop),
    Queue(ReplacementQueueError),
    Send(SingleSendError<ReplacementAdmissionReceipt>),
}

/// Submit one frozen Acquire generation from the existing qualification
/// future. The caller supplies its original cancellation receiver and
/// absolute expiry. After launch the caller must drive this future through
/// retirement, including when cancellation or expiry wins the handoff.
pub(crate) async fn send_replacement_admission(
    context: SingleSendContext<'_>,
    qualification: &WorkObligation,
    generation: Generation,
    request: AcquireParticipationAdmissionTicket,
    key: SendKey,
    cancellation: &mut watch::Receiver<bool>,
    expiry: Instant,
) -> Result<ReplacementAdmissionSend, ReplacementSendError> {
    if *cancellation.borrow() {
        return Err(ReplacementSendError::StoppedBeforeHandoff(
            ReplacementSendStop::Cancelled,
        ));
    }
    if expiry <= Instant::now() {
        return Err(ReplacementSendError::StoppedBeforeHandoff(
            ReplacementSendStop::Expired,
        ));
    }
    if !context.scope.same_scope(qualification.scope()) {
        return Err(ReplacementSendError::Send(SingleSendError::WrongOwner));
    }
    let ledger = context.ledger;
    let layout = context.layout;
    let operation = OperationIntent::AcquireParticipationAdmissionTicket(request);
    let prepared =
        PreparedSingleSend::new(context, operation, key).map_err(ReplacementSendError::Send)?;
    let admission = prepared
        .admit()
        .map_err(|error| ReplacementSendError::Send(SingleSendError::Governance(error)))?;
    let input = tokio::select! {
        biased;
        stop = wait_for_stop(cancellation, expiry) => {
            return Err(ReplacementSendError::StoppedBeforeHandoff(stop));
        }
        original = admission => original
            .map_err(|error| ReplacementSendError::Send(SingleSendError::Governance(error)))?,
    };
    let (guard, _) = tokio::select! {
        biased;
        stop = wait_for_stop(cancellation, expiry) => {
            return Err(ReplacementSendError::StoppedBeforeHandoff(stop));
        }
        original = reserve_replacement_send(ledger, layout, qualification, generation, request, key, &input) => {
            original.map_err(ReplacementSendError::Queue)?
        }
    };
    let mut stopped = None;
    let original = prepared
        .run(
            guard,
            input,
            generation,
            expiry,
            async { stopped = Some(wait_for_stop(cancellation, expiry).await) },
            ReplacementAdmissionReceipt::observe,
        )
        .await
        .map_err(ReplacementSendError::Send)?;
    Ok(ReplacementAdmissionSend { original, stopped })
}

pub(crate) async fn wait_for_stop(
    cancellation: &mut watch::Receiver<bool>,
    expiry: Instant,
) -> ReplacementSendStop {
    if *cancellation.borrow() {
        return ReplacementSendStop::Cancelled;
    }
    tokio::select! {
        biased;
        _ = cancellation.changed() => ReplacementSendStop::Cancelled,
        _ = tokio::time::sleep_until(expiry) => ReplacementSendStop::Expired,
    }
}

#[derive(Debug)]
pub(crate) enum ReplacementQueueError {
    WrongOriginalInput,
    Layout(TransportCapacityError),
    Runtime(RuntimeError),
}

/// Wait for the same original process and keyed queue windows, keeping the
/// admitted WC input in the Source's custody until there is an actual guard.
/// The Source must keep the same qualification parent live while this future
/// is pending. The ledger verifies the registrant's origin in the reservation
/// transaction. A grant from a previous replay generation cannot enter it.
///
/// The caller races this future against the original cancellation and expiry.
/// Dropping the future unregisters the capacity waiter. Dropping a returned
/// unused guard rolls back its original reservation, which must happen before
/// releasing the paired WC input. If the guard is bound, its original Source
/// takes over both obligations through the normal transport custody path.
#[must_use = "the returned guard must enter the original Source or roll back before WC queue exit"]
pub(crate) async fn reserve_replacement_send(
    ledger: &RuntimeLedger,
    layout: &NativeTransportCapacity,
    qualification: &WorkObligation,
    generation: Generation,
    request: AcquireParticipationAdmissionTicket,
    send: SendKey,
    input: &OriginalQueuedInput,
) -> Result<(StepRegistration, WorkKind), ReplacementQueueError> {
    if send.operation != request.envelope().operation_id() || input.key() != send {
        return Err(ReplacementQueueError::WrongOriginalInput);
    }
    let intent = OperationIntent::AcquireParticipationAdmissionTicket(request);
    let (lane, kind) = if intent.requires_control_progress() {
        (TransportCapacityLane::Control, WorkKind::Settlement)
    } else {
        (TransportCapacityLane::Ordinary, WorkKind::Business)
    };
    let resources = send_queue_resources(
        layout,
        intent.backend_process_id(),
        lane,
        1,
        intent.queued_bytes(),
    )
    .map_err(ReplacementQueueError::Layout)?;
    let mut registrant: Option<CapacityRegistrant> = None;
    poll_fn(|cx| {
        let registrant = match registrant.as_mut() {
            Some(original) => {
                original.update_waker(cx.waker());
                original
            }
            None => match ledger.capacity_registrant(cx.waker().clone()) {
                Ok(original) => registrant.insert(original),
                Err(error) => return Poll::Ready(Err(ReplacementQueueError::Runtime(error))),
            },
        };
        match qualification.scope().reserve_next_or_wait(
            generation,
            resources.clone(),
            kind,
            WorkAdmission::Derived(qualification.id()),
            registrant,
        ) {
            Ok(original) => Poll::Ready(Ok((original, kind))),
            Err(RuntimeError::CapacityDenied(denial)) if denial.requested <= denial.limit => {
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(ReplacementQueueError::Runtime(error))),
        }
    })
    .await
}

#[cfg(test)]
mod tests;
