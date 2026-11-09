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

//! Queue-stage admission for the non-actor replacement qualification Source.
//!
//! The logical owner supplies its original immutable admission request and
//! one WC input for this send. Retry, cancellation, expiry and settlement
//! stay with that owner. This boundary reserves only the original shared FE
//! queue stage. Its result goes to the Source's ordinary SendBatch binding.

use super::transport::{OriginalQueuedInput, SendKey};
use crate::native::transport_capacity::{
    NativeTransportCapacity, TransportCapacityError, TransportCapacityLane,
};
use crate::task_execution::intent::OperationIntent;
use novarocks_control_runtime::{
    capacity_notification::CapacityRegistrant,
    obligation::StepRegistration,
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger},
};
use novarocks_control_runtime_core::{
    identity::Generation,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::AcquireParticipationAdmissionTicket;
use std::{future::poll_fn, task::Poll};

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
    qualification: &ReportedWork,
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
    let mut resources = layout
        .queue_resources(intent.backend_process_id(), lane, 1, intent.queued_bytes())
        .map_err(ReplacementQueueError::Layout)?;
    // queue_resources already prices the Work header. This send's one
    // producer report and capture position travel with its original Work.
    resources.owned_slots.completions = 1;
    resources.owned_slots.captures = 1;
    let parent = qualification.work();
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
        match parent.scope().reserve_next_or_wait(
            generation,
            resources.clone(),
            kind,
            WorkAdmission::Derived(parent.id()),
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
