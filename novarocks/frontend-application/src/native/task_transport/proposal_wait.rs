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

//! One notification F in the original bounded operation sink.
use super::*;
use novarocks_workload_control::{
    WorkError,
    joint_admission::{JointProposalSlotSignal, JointProposalSlotWait},
};
use std::{alloc::Layout, pin::Pin, task::Context};

#[derive(Default)]
enum WaitState {
    #[default]
    Empty,
    Waiting(JointProposalSlotWait),
    Polling {
        observed: bool,
    },
    // Never repoll a notification F whose poll unwound. Its original source
    // remains observable until the normal Source owner disposes that source.
    Poisoned(Option<JointProposalSlotWait>),
}
#[derive(Default)]
pub(super) struct OriginalProposalWait {
    state: Mutex<WaitState>,
    #[cfg(test)]
    created: std::sync::atomic::AtomicUsize,
}
struct BorrowedWait<'a> {
    source: &'a OriginalProposalWait,
    wake: &'a TaskAckIntakeInner,
    wait: Option<JointProposalSlotWait>,
    completed: bool,
}
impl BorrowedWait<'_> {
    fn finish(&mut self, pending: bool) {
        if !pending {
            // Keep the original registration position unavailable during the
            // actual F/waker destruction; no successor F may overlap it.
            drop(self.wait.take());
        }
        let observed = {
            let mut state = self.source.state.lock().unwrap_or_else(|e| e.into_inner());
            let WaitState::Polling { observed } = &*state else {
                unreachable!("one source borrow")
            };
            let observed = *observed;
            *state = if pending {
                WaitState::Waiting(self.wait.take().unwrap())
            } else {
                WaitState::Empty
            };
            observed
        };
        self.completed = true;
        if observed {
            self.wake.wake.wake();
        }
    }
}
impl Drop for BorrowedWait<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut state = self.source.state.lock().unwrap_or_else(|e| e.into_inner());
        // The unwound F is restored to the SAME fixed source field and is
        // never polled again. The original root catch owns the actual cause.
        *state = WaitState::Poisoned(self.wait.take());
    }
}
impl OriginalProposalWait {
    pub(super) fn poll(
        &self,
        signal: &JointProposalSlotSignal,
        cx: &mut Context<'_>,
        wake: &TaskAckIntakeInner,
    ) -> Poll<Result<(), WorkError>> {
        // This is only an observation callback. Both real caller sites pass
        // the SAME original intake Arc waker, so another preparation cannot
        // replace a different actor's notification registration.
        let mut borrowed = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            match &mut *state {
                WaitState::Polling { observed } => {
                    *observed = true;
                    return Poll::Pending;
                }
                WaitState::Poisoned(_) => return Poll::Ready(Err(WorkError::Conflict)),
                WaitState::Waiting(wait) if !wait.is_for_signal(signal) => {
                    return Poll::Ready(Err(WorkError::Conflict));
                }
                WaitState::Waiting(_) | WaitState::Empty => {}
            }
            let old = std::mem::replace(&mut *state, WaitState::Polling { observed: false });
            BorrowedWait {
                source: self,
                wake,
                wait: match old {
                    WaitState::Waiting(wait) => Some(wait),
                    WaitState::Empty => None,
                    _ => unreachable!(),
                },
                completed: false,
            }
        };
        // This is the sole allocation site, after the one fixed source field
        // is reserved. A Full signal itself never allocates a wait F.
        if borrowed.wait.is_none() {
            borrowed.wait = Some(signal.clone().into_wait());
            #[cfg(test)]
            self.created
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let result = Pin::new(borrowed.wait.as_mut().unwrap()).poll(cx);
        borrowed.finish(result.is_pending());
        result
    }
    #[cfg(test)]
    pub(super) fn created_wait_futures(&self) -> usize {
        self.created.load(std::sync::atomic::Ordering::Relaxed)
    }
    #[cfg(test)]
    pub(super) fn has_wait_future(&self) -> bool {
        matches!(
            &*self.state.lock().unwrap_or_else(|e| e.into_inner()),
            WaitState::Waiting(_) | WaitState::Poisoned(Some(_))
        )
    }
    pub(super) fn retire(&mut self) {
        let state = std::mem::take(self.state.get_mut().unwrap_or_else(|e| e.into_inner()));
        // NativeTaskOperationSink::drop calls this while its original intake
        // Arc and registration lease are still alive. F/last waker exits first.
        match state {
            WaitState::Waiting(wait) => drop(wait),
            WaitState::Poisoned(wait) => drop(wait),
            WaitState::Empty => {}
            WaitState::Polling { .. } => unreachable!("source cannot exit while borrowed"),
        }
    }
}
// A failed real F destruction cannot return the original finite Source
// registration. The actual root panic receiver gets this same lease/cause.
// This is exceptional custody, not a normal shutdown/actual-exit receipt.
pub(super) struct OriginalWaitExitFailure {
    cause: Option<Box<dyn std::any::Any + Send>>,
    lease: Option<NativeTransportWaiter>,
}
impl OriginalWaitExitFailure {
    pub(super) fn new(cause: Box<dyn std::any::Any + Send>, lease: NativeTransportWaiter) -> Self {
        Self {
            cause: Some(cause),
            lease: Some(lease),
        }
    }
}
impl Drop for OriginalWaitExitFailure {
    fn drop(&mut self) {
        let cause = self.cause.take();
        if let Err(cause) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(cause))) {
            let lease = self.lease.take().expect("one original source lease");
            std::panic::resume_unwind(Box::new(Self::new(cause, lease)));
        }
        drop(self.lease.take());
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeProposalWaitSourceGeometry {
    pub(crate) records: usize,
    pub(crate) slot: Layout,
    pub(crate) future: Layout,
    pub(crate) exit_failure: Layout,
}
impl NativeProposalWaitSourceGeometry {
    pub(crate) fn resident_bytes(self) -> Result<usize, WorkError> {
        self.exit_failure
            .size()
            .checked_mul(2)
            .and_then(|n| n.checked_add(self.slot.size()))
            .and_then(|n| n.checked_add(self.future.size()))
            .and_then(|n| n.checked_mul(self.records))
            .ok_or(WorkError::ArithmeticOverflow)
    }
}
impl NativeTaskOperationSink {
    pub(crate) fn proposal_wait_source_geometry(
        budget: TransportBudget,
    ) -> Result<NativeProposalWaitSourceGeometry, WorkError> {
        let records = super::super::transport_supervisor::NativeTransportSupervisor::source_waiter_records_for_transport(budget)
            .map_err(|_| WorkError::InvalidConfig("Native source waiter geometry"))?;
        Ok(NativeProposalWaitSourceGeometry {
            records,
            slot: Layout::new::<OriginalProposalWait>(),
            future: JointProposalSlotWait::metadata_layouts().1,
            exit_failure: Layout::new::<OriginalWaitExitFailure>(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_source_quote_uses_original_registration_limit_and_actual_wait_factory() {
        let budget = TransportBudget::DEFAULT;
        let quote = NativeTaskOperationSink::proposal_wait_source_geometry(budget).unwrap();
        let original =
            super::super::super::transport_supervisor::NativeTransportSupervisor::from_transport(
                budget,
            )
            .unwrap();
        assert_eq!(quote.records, original.capacity_sources().waiter_records);
        assert_eq!(quote.slot, Layout::new::<OriginalProposalWait>());
        assert_eq!(quote.future, JointProposalSlotWait::metadata_layouts().1);
        assert_eq!(
            quote.resident_bytes().unwrap(),
            quote.records
                * (quote.slot.size() + quote.future.size() + 2 * quote.exit_failure.size())
        );
    }
}
