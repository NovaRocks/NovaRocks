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

//! Real WC queued-input admission, without a second Native transport window.
use super::{capacity::QueueCustody, proposal_wait::ProposalWait, source_ports::TransportReports};
use novarocks_attempt_protocol::input::SendGeneration;
use novarocks_execution_contract::TaskOperationId;
use novarocks_workload_control::{
    WorkError,
    joint_admission::{
        JointLane, NativeAsyncActionAuthority, NativeQueuedInputAdmission, NativeQueuedInputGrant,
        NativeQueuedInputQuote, NativeQueuedInputReady,
    },
};
use std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
};

/// A passive address within the original Source. It never issues WC, Work,
/// physical backing or exit authority from these identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SendKey {
    pub(crate) operation: TaskOperationId,
    pub(crate) generation: SendGeneration,
}

/// An admitted input not yet associated with any physical queue reservation.
/// Source must consume it into a SendInputLoan at the actual queue binding,
/// before publishing or executing that send. Only this never-physical state
/// may directly exit its WC queue on withdrawal.
/// The same paid WC input survives until original queue/actual-send input exit.
/// Its payload aliases retain their independent original backing after queue
/// exit. This wrapper grants no process/keyed capacity and mints no author.
pub(crate) struct OriginalQueuedInput {
    key: SendKey,
    input: Option<NativeQueuedInputGrant>,
}
impl OriginalQueuedInput {
    pub(crate) fn key(&self) -> SendKey {
        self.key
    }
    /// First creation's canonical owner takes this same payload-only pin before
    /// exposing the frozen request. Later generations quote no new payload;
    /// their queue grants never substitute for this original retention.
    pub(crate) fn retain_payload(
        &self,
    ) -> Result<
        novarocks_resource_contract::ExistingOwnerRetentionPin,
        novarocks_resource_contract::RetentionPinError,
    > {
        self.borrow().retain_payload()
    }
    /// Requires the actual original queue object, not a bool/descriptor proof.
    /// The Source binds this loan only after associating that reservation with
    /// its same canonical (operation, send-generation) record.
    pub(crate) fn bind_send(
        mut self,
        _queue: &QueueCustody,
        source: &Arc<dyn TransportReports>,
    ) -> SendInputLoan {
        SendInputLoan {
            input: Some(ReturnedQueuedInput {
                key: self.key,
                input: ManuallyDrop::new(self.input.take().expect("same original WC input")),
            }),
            source: Arc::downgrade(source),
        }
    }

    pub(crate) fn borrow(&self) -> &NativeQueuedInputGrant {
        self.input
            .as_ref()
            .expect("original queued input not moved")
    }
}
struct QueueExitUnproven {
    _cause: WorkError,
    _input: ManuallyDrop<NativeQueuedInputGrant>,
}
impl Drop for OriginalQueuedInput {
    fn drop(&mut self) {
        let Some(input) = self.input.take() else {
            return;
        };
        match input.queue_exited_in() {
            Ok(notification) => {
                drop(input);
                notification.notify();
            }
            Err(cause) => std::panic::resume_unwind(Box::new(QueueExitUnproven {
                _cause: cause,
                _input: ManuallyDrop::new(input),
            })),
        }
    }
}
/// The exact loan returned before an Apply row is attached. Drop deliberately
/// retains the original grant as UNKNOWN: it cannot certify that this send's
/// queue/encoding reservations actually finished. The original Source alone
/// consumes it after its same Join and both financial legs really exit.
#[must_use = "The original Source must settle or retain this physical input"]
pub(crate) struct ReturnedQueuedInput {
    key: SendKey,
    input: ManuallyDrop<NativeQueuedInputGrant>,
}
impl ReturnedQueuedInput {
    pub(crate) fn key(&self) -> SendKey {
        self.key
    }
    pub(crate) fn borrow(&self) -> &NativeQueuedInputGrant {
        &self.input
    }
    /// Move the same grant to the Source's original Native row. This transfers
    /// custody only, never signals queue exit or financial return.
    pub(crate) fn into_original(self) -> NativeQueuedInputGrant {
        ManuallyDrop::into_inner(self.input)
    }
    /// Caller is the original Source after actual Join, Native return (or its
    /// real never-attached boundary), and successful queue+encoding finishes.
    /// Failure returns the same unexited object; no destructor refunds it.
    pub(crate) fn finish_after_physical_exit(self) -> Result<(), (WorkError, Self)> {
        match self.input.queue_exited_in() {
            Ok(notification) => {
                drop(ManuallyDrop::into_inner(self.input));
                notification.notify();
                Ok(())
            }
            Err(error) => Err((error, self)),
        }
    }
}
/// Move-only loan of that same Source's grant, not a parallel queue record.
/// Withdrawal first destroys the pre-handoff loan; its only effect is to park
/// the actual object in the Source's prepaid return cell. No queue exit occurs.
pub(crate) struct SendInputLoan {
    input: Option<ReturnedQueuedInput>,
    source: Weak<dyn TransportReports>,
}
impl SendInputLoan {
    pub(crate) fn key(&self) -> SendKey {
        self.input.as_ref().expect("original input loan").key()
    }
    pub(crate) fn borrow(&self) -> &NativeQueuedInputGrant {
        self.input.as_ref().expect("original input loan").borrow()
    }
    pub(crate) fn into_original(mut self) -> NativeQueuedInputGrant {
        self.input
            .take()
            .expect("same original input")
            .into_original()
    }
}
struct UnprovenInputReturn {
    _input: ReturnedQueuedInput,
}
impl Drop for SendInputLoan {
    fn drop(&mut self) {
        let Some(input) = self.input.take() else {
            return;
        };
        let Some(source) = self.source.upgrade() else {
            std::panic::resume_unwind(Box::new(UnprovenInputReturn { _input: input }));
        };
        if let Err(input) = source.stage_input(input) {
            std::panic::resume_unwind(Box::new(UnprovenInputReturn { _input: input }));
        }
    }
}

/// This future itself is the GatedWork pre-handoff object. Withdrawal drops
/// it first, including the same ready/grant and proposal observer, before the
/// outer work's captures and revoke_unused_and_exit are processed.
pub(crate) struct AdmitQueuedInput {
    key: SendKey,
    admission: NativeQueuedInputAdmission,
    ready: Option<NativeQueuedInputReady>,
    grant: Option<NativeQueuedInputGrant>,
    proposal: ProposalWait,
}
impl AdmitQueuedInput {
    pub(crate) fn new(
        key: SendKey,
        authority: &NativeAsyncActionAuthority,
        control: bool,
        quote: NativeQueuedInputQuote,
    ) -> Result<Self, WorkError> {
        let admission = authority
            .begin_queued_input_when_available(
                if control {
                    JointLane::Settlement
                } else {
                    JointLane::Foreground
                },
                quote,
            )
            .map_err(|failure| failure.error)?;
        Ok(Self {
            key,
            admission,
            ready: None,
            grant: None,
            proposal: ProposalWait::default(),
        })
    }
}
impl Future for AdmitQueuedInput {
    type Output = Result<OriginalQueuedInput, WorkError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.ready.is_none() {
            let proposal = &mut this.proposal;
            match this
                .admission
                .poll_with_slot_observer(cx, &mut |signal, cx| proposal.poll(signal, cx))
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(failure)) => return Poll::Ready(Err(failure.error)),
                Poll::Ready(Ok(ready)) => this.ready = Some(ready),
            }
        }
        let notification = match this
            .ready
            .as_mut()
            .expect("original ready paid input")
            .install_in(&mut this.grant)
        {
            Ok(n) => n,
            Err(error) => return Poll::Ready(Err(error)),
        };
        // Publish every actual field before a foreign wake can observe it.
        drop(this.ready.take());
        let input = OriginalQueuedInput {
            key: this.key,
            input: this.grant.take(),
        };
        notification.notify();
        Poll::Ready(Ok(input))
    }
}

impl Drop for AdmitQueuedInput {
    fn drop(&mut self) {
        // An adopted input that never reached the Source still has an actual
        // queue holder. Unadopted ready claims retain their own normal Drop.
        if self.grant.is_some() {
            drop(OriginalQueuedInput {
                key: self.key,
                input: self.grant.take(),
            });
        }
    }
}

#[cfg(test)]
mod tests;
