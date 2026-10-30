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

//! Original producer-owned paid input preparation, before the process gate.
use super::*;
use crate::task_execution::intent::{
    TaskOperationQueuePermit, TaskOperationQueuePreparation, TaskOperationQueueRequest,
};
use novarocks_resource_contract::ExistingOwnerRetentionLease;
use novarocks_workload_control::{
    WorkError,
    joint_admission::{JointLane, NativeQueuedInputGrant, NativeQueuedInputQuote},
};
use std::{
    alloc::Layout,
    task::{Context, Poll, Wake, Waker},
};

impl Wake for TaskAckIntakeInner {
    fn wake(self: Arc<Self>) {
        self.wake.wake()
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.wake.wake()
    }
}

/// Original queue row and its source guard. The Box header itself belongs to
/// the original fixed process queue inventory, including a retirement tail.
pub(super) struct NativeInputQueuePermit {
    physical: Option<super::super::transport_supervisor::NativeTransportQueuePermit>,
    input: Option<NativeQueuedInputGrant>,
}
impl std::fmt::Debug for NativeInputQueuePermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OriginalPaidNativeQueuePermit")
    }
}
impl TaskOperationQueuePermit for NativeInputQueuePermit {
    fn mark_in_flight(&mut self) {
        self.physical
            .as_mut()
            .expect("original process queue holder")
            .mark_in_flight()
    }
    fn native_dispatch_input(&self) -> Option<&NativeQueuedInputGrant> {
        self.input.as_ref()
    }
    fn native_input_retention(&self) -> Result<Option<ExistingOwnerRetentionLease>, WorkError> {
        self.input
            .as_ref()
            .ok_or(WorkError::Released)?
            .retain_payload()
            .map(Some)
            .map_err(|error| match error {
                novarocks_resource_contract::RetentionPinError::Released => WorkError::Released,
                novarocks_resource_contract::RetentionPinError::Conflict => WorkError::Conflict,
                novarocks_resource_contract::RetentionPinError::Overflow => {
                    WorkError::ArithmeticOverflow
                }
            })
    }
}
struct OriginalQueueCleanupFailure {
    // The actual cause exits before the still-original input guard. No
    // arbitrary payload is copied or formatted into a synthetic cause.
    _cause: Box<dyn std::any::Any + Send>,
    _input: NativeQueuedInputGrant,
}
impl Drop for NativeInputQueuePermit {
    fn drop(&mut self) {
        // Empty each original field before foreign destruction. A physical
        // Drop unwind moves the same input into the actual raw panic object;
        // ordinary field cleanup cannot manufacture its queue-exit proof.
        let physical = self.physical.take();
        let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(physical)));
        let Some(input) = self.input.take() else {
            return;
        };
        if let Err(cause) = actual {
            std::panic::resume_unwind(Box::new(OriginalQueueCleanupFailure {
                _cause: cause,
                _input: input,
            }));
        }
        match input.queue_exited_in() {
            Ok(notification) => {
                // Queue exit is now durable. Independent payload aliases keep
                // their original bytes, including during a wake unwind.
                drop(input);
                notification.notify();
            }
            Err(error) => {
                std::panic::resume_unwind(Box::new(OriginalQueueCleanupFailure {
                    _cause: Box::new(error),
                    _input: input,
                }));
            }
        }
    }
}
fn metadata(bytes: usize) -> Result<std::num::NonZeroU64, WorkError> {
    std::num::NonZeroU64::new(u64::try_from(bytes).map_err(|_| WorkError::ArithmeticOverflow)?)
        .ok_or(WorkError::Conflict)
}
pub(crate) fn queued_header_quote(payload: usize) -> Result<NativeQueuedInputQuote, WorkError> {
    let bytes = Layout::new::<NativeInputQueuePermit>()
        .size()
        .checked_add(crate::task_execution::dispatch::queued_operation_layout().size())
        .ok_or(WorkError::ArithmeticOverflow)?;
    Ok(NativeQueuedInputQuote::for_original_native_input(
        metadata(bytes)?,
        u64::try_from(payload).map_err(|_| WorkError::ArithmeticOverflow)?,
    ))
}
pub(crate) fn queued_creation_quote(
    new_backing: usize,
) -> Result<NativeQueuedInputQuote, WorkError> {
    let headers = crate::task_execution::creation::creation_owner_header_bytes().map_err(
        |error| match error {
            crate::task_execution::error::TaskExecutionError::Workload(error) => error,
            _ => WorkError::Conflict,
        },
    )?;
    queued_header_quote(
        new_backing
            .checked_add(headers)
            .ok_or(WorkError::ArithmeticOverflow)?,
    )
}
pub(crate) fn queued_intent_quote(
    _intent: &OperationIntent,
) -> Result<NativeQueuedInputQuote, WorkError> {
    // Immutable original content already has its own creator/retention owner.
    // Enqueue clones only those existing aliases. DTO/encoding copies belong
    // to the later actual action quote, not to an encoded-length byte guess.
    queued_header_quote(0)
}
pub(crate) fn queued_update_quote(
    _update: &novarocks_execution::task_execution::TaskDomainUpdate,
) -> Result<NativeQueuedInputQuote, WorkError> {
    queued_header_quote(0)
}

impl NativeTaskOperationSink {
    pub(super) fn reserve_paid_queue_in(
        &self,
        request: TaskOperationQueueRequest,
        preparation: &mut TaskOperationQueuePreparation,
    ) -> TaskOperationQueueAdmission {
        if let Some(error) = preparation.failure.as_ref() {
            return TaskOperationQueueAdmission::Rejected(error.clone());
        }
        if preparation
            .request
            .is_some_and(|original| original != request)
        {
            return TaskOperationQueueAdmission::Rejected(WorkError::Conflict);
        }
        if preparation.request.is_none() {
            preparation.request = Some(request)
        }
        if preparation.admission.is_none()
            && preparation.ready.is_none()
            && preparation.grant.is_none()
        {
            let Some(quote) = request.native_input else {
                return TaskOperationQueueAdmission::Rejected(WorkError::Conflict);
            };
            // The original grant retains this actual creation/new-backing
            // quote; the process/target queue budget is reserved below.
            let lane = if request.requires_control_progress() {
                JointLane::Settlement
            } else {
                JointLane::Foreground
            };
            match self
                .authority
                .begin_queued_input_when_available(lane, quote)
            {
                Ok(admission) => preparation.admission = Some(admission),
                Err(error) => {
                    preparation.failure = Some(error.error.clone());
                    return TaskOperationQueueAdmission::Rejected(error.error);
                }
            }
        }
        let waker = Waker::from(Arc::clone(&self.acks.inner));
        let mut cx = Context::from_waker(&waker);
        if let Some(admission) = preparation.admission.as_mut() {
            match admission.poll_with_slot_observer(&mut cx, &mut |signal, cx| {
                self.proposal_wait.poll(signal, cx, &self.acks.inner)
            }) {
                Poll::Pending => {
                    return if admission.is_waiting_for_proposal_slot() {
                        // Original Root-wide Q exhaustion stops this producer pass
                        // before any other unpublished input allocates a waiter.
                        TaskOperationQueueAdmission::ProcessFull
                    } else {
                        TaskOperationQueueAdmission::TargetFull
                    };
                }
                Poll::Ready(Err(error)) => {
                    preparation.failure = Some(error.error.clone());
                    return TaskOperationQueueAdmission::Rejected(error.error);
                }
                Poll::Ready(Ok(ready)) => {
                    preparation.ready = Some(ready);
                    preparation.admission = None;
                }
            }
        }
        if let Some(ready) = preparation.ready.as_mut() {
            let notification = match ready.install_in(&mut preparation.grant) {
                Ok(n) => n,
                Err(error) => {
                    preparation.failure = Some(error.clone());
                    return TaskOperationQueueAdmission::Rejected(error);
                }
            };
            // The exact source field is stable before any callback.
            preparation.ready = None;
            notification.notify();
        }
        let lane = if request.requires_control_progress() {
            NativeTransportLane::Control
        } else {
            NativeTransportLane::Ordinary
        };
        match self
            .data_runtime
            .task_transport_supervisor()
            .try_reserve_queue(
                &self.transport_waiter,
                request.backend_process_id(),
                lane,
                1,
                request.queued_bytes(),
            ) {
            Ok(physical) => {
                let input = preparation
                    .grant
                    .take()
                    .expect("original paid input installed");
                preparation.request = None;
                TaskOperationQueueAdmission::Admitted(Box::new(NativeInputQueuePermit {
                    physical: Some(physical),
                    input: Some(input),
                }))
            }
            Err(NativeTransportBackpressure::Target) => TaskOperationQueueAdmission::TargetFull,
            Err(NativeTransportBackpressure::Process) => TaskOperationQueueAdmission::ProcessFull,
        }
    }
}
